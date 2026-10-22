//! IOKit power assertion that blocks system sleep, the system-wide assertion
//! that says someone is using the host, and whether the host has a battery.

use std::ffi::{CStr, c_char, c_void};

type CfStringRef = *const c_void;
type IoPmAssertionId = u32;
type IoReturn = i32;

const K_CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
const K_IOPM_ASSERTION_LEVEL_ON: u32 = 255;
const K_IO_RETURN_SUCCESS: IoReturn = 0;
// IOReturn codes are 32-bit patterns; the wrap to i32 is intended
const K_IO_RETURN_BAD_ARGUMENT: IoReturn = 0xE000_02C2_u32 as IoReturn;
// the assertion `caffeinate -s` takes; any user may create it. The weaker
// PreventUserIdleSystemSleep does not stop a dark wake from going back to
// sleep, so a host that was asleep when the daemon started stays asleep
const PREVENT_SYSTEM_SLEEP: &CStr = c"PreventSystemSleep";
// powerd holds this while someone uses the host and drops it when the display
// sleeps for idleness; a dark wake never raises it
const USER_IS_ACTIVE: &CStr = c"UserIsActive";
const K_CF_NUMBER_SINT32_TYPE: isize = 3;
// `kIOPSTypeKey` and `kIOPSInternalBatteryType`; a UPS reports another type
const POWER_SOURCE_TYPE: &CStr = c"Type";
const INTERNAL_BATTERY: &CStr = c"InternalBattery";

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    fn CFStringCreateWithCString(
        alloc: *const c_void,
        c_str: *const c_char,
        encoding: u32,
    ) -> CfStringRef;
    fn CFRelease(cf: *const c_void);
    fn CFDictionaryGetValue(dict: *const c_void, key: *const c_void) -> *const c_void;
    fn CFNumberGetValue(number: *const c_void, number_type: isize, value: *mut c_void) -> u8;
    fn CFArrayGetCount(array: *const c_void) -> isize;
    fn CFArrayGetValueAtIndex(array: *const c_void, index: isize) -> *const c_void;
    fn CFEqual(left: *const c_void, right: *const c_void) -> u8;
}

#[link(name = "IOKit", kind = "framework")]
unsafe extern "C" {
    fn IOPMAssertionCreateWithName(
        assertion_type: CfStringRef,
        level: u32,
        name: CfStringRef,
        assertion_id: *mut IoPmAssertionId,
    ) -> IoReturn;
    fn IOPMAssertionRelease(assertion_id: IoPmAssertionId) -> IoReturn;
    fn IOPMCopyAssertionsStatus(status: *mut *const c_void) -> IoReturn;
    fn IOPSCopyPowerSourcesInfo() -> *const c_void;
    fn IOPSCopyPowerSourcesList(blob: *const c_void) -> *const c_void;
    fn IOPSGetPowerSourceDescription(blob: *const c_void, source: *const c_void) -> *const c_void;
}

/// Owned `CFStringRef`, released on drop.
struct CfString(CfStringRef);

impl CfString {
    fn new(text: &CStr) -> Option<Self> {
        // SAFETY: `text` is a valid NUL-terminated string for the call, and a
        // null allocator selects the default one
        let raw = unsafe {
            CFStringCreateWithCString(std::ptr::null(), text.as_ptr(), K_CF_STRING_ENCODING_UTF8)
        };
        Self::from_created(raw)
    }

    fn from_created(raw: CfStringRef) -> Option<Self> {
        (!raw.is_null()).then(|| Self(raw))
    }
}

impl Drop for CfString {
    fn drop(&mut self) {
        // SAFETY: `self.0` came from a Create function and is released once
        unsafe { CFRelease(self.0) };
    }
}

/// A held `PreventSystemSleep` assertion, released on drop.
#[derive(Debug)]
pub(super) struct SystemSleepAssertion(IoPmAssertionId);

impl SystemSleepAssertion {
    /// Create the assertion. `name` shows in `pmset -g assertions`. The error
    /// is the `IOReturn` code.
    pub(super) fn create(name: &CStr) -> Result<Self, IoReturn> {
        let assertion_type = CfString::new(PREVENT_SYSTEM_SLEEP).ok_or(K_IO_RETURN_BAD_ARGUMENT)?;
        let name = CfString::new(name).ok_or(K_IO_RETURN_BAD_ARGUMENT)?;
        let mut id: IoPmAssertionId = 0;
        // SAFETY: both strings are live CFStrings for the call, and `id` is a
        // valid out pointer
        let code = unsafe {
            IOPMAssertionCreateWithName(
                assertion_type.0,
                K_IOPM_ASSERTION_LEVEL_ON,
                name.0,
                &mut id,
            )
        };
        if code != K_IO_RETURN_SUCCESS {
            return Err(code);
        }
        Ok(Self(id))
    }
}

impl Drop for SystemSleepAssertion {
    fn drop(&mut self) {
        // SAFETY: the id came from a successful create and is released once;
        // a failed release leaves nothing to clean up, and process exit
        // releases it anyway
        unsafe { IOPMAssertionRelease(self.0) };
    }
}

/// Whether the host is fully awake rather than in a dark wake, or `None`
/// when IOKit does not say
///
/// A full wake has graphics; a dark wake runs with CPU, network, and disk
/// only. `IOPMConnectionGetSystemCapabilities` is exported by IOKit but left
/// out of the public headers, so it is looked up at run time: a macOS that
/// drops it then leaves this unknown instead of stopping the daemon from loading
pub(super) fn fully_awake() -> Option<bool> {
    // `kIOPMCapabilityVideo` in Apple's PowerManagement sources
    const CAPABILITY_VIDEO: u32 = 0x2;
    // SAFETY: the name is NUL-terminated, and RTLD_DEFAULT searches the
    // images already loaded, which include IOKit
    let symbol = unsafe {
        libc::dlsym(
            libc::RTLD_DEFAULT,
            c"IOPMConnectionGetSystemCapabilities".as_ptr(),
        )
    };
    if symbol.is_null() {
        return None;
    }
    // SAFETY: the symbol is IOKit's `IOPMCapabilityBits
    // IOPMConnectionGetSystemCapabilities(void)`, a 32-bit mask
    let capabilities =
        unsafe { std::mem::transmute::<*mut c_void, unsafe extern "C" fn() -> u32>(symbol)() };
    Some(capabilities & CAPABILITY_VIDEO != 0)
}

/// Whether someone is using the host now, from the system-wide `UserIsActive`
/// assertion that `pmset -g assertions` lists. `None` when IOKit does not say
///
/// The assertion can outlast the display, so a caller also checks [`fully_awake`]
pub(super) fn user_is_active() -> Option<bool> {
    let key = CfString::new(USER_IS_ACTIVE)?;
    let mut status: *const c_void = std::ptr::null();
    // SAFETY: `status` is a valid out pointer; on success it receives a
    // dictionary this function owns
    let code = unsafe { IOPMCopyAssertionsStatus(&mut status) };
    if code != K_IO_RETURN_SUCCESS {
        return None;
    }
    let status = Owned::new(status)?;
    // SAFETY: `status` is a live CFDictionary and `key` a live CFString; the
    // value is borrowed from the dictionary, which outlives the read
    let number = unsafe { CFDictionaryGetValue(status.0, key.0) };
    if number.is_null() {
        return None;
    }
    let mut level: i32 = 0;
    // SAFETY: `number` is a CFNumber from the dictionary, and `level` is a
    // valid out pointer for a signed 32-bit value
    let read =
        unsafe { CFNumberGetValue(number, K_CF_NUMBER_SINT32_TYPE, (&raw mut level).cast()) };
    (read != 0).then_some(level != 0)
}

/// Owned CoreFoundation object from a Copy function, released on drop
struct Owned(*const c_void);

impl Owned {
    fn new(raw: *const c_void) -> Option<Self> {
        (!raw.is_null()).then(|| Self(raw))
    }
}

impl Drop for Owned {
    fn drop(&mut self) {
        // SAFETY: `self.0` came from a Copy function and is released once
        unsafe { CFRelease(self.0) };
    }
}

/// Whether one of the host's power sources is an internal battery, as on a laptop
pub(super) fn has_internal_battery() -> bool {
    let (Some(key), Some(battery)) = (
        CfString::new(POWER_SOURCE_TYPE),
        CfString::new(INTERNAL_BATTERY),
    ) else {
        return false;
    };
    // SAFETY: takes no arguments and returns an owned snapshot or null
    let Some(info) = Owned::new(unsafe { IOPSCopyPowerSourcesInfo() }) else {
        return false;
    };
    // SAFETY: `info` is a live power-sources snapshot
    let Some(list) = Owned::new(unsafe { IOPSCopyPowerSourcesList(info.0) }) else {
        return false;
    };
    // SAFETY: `list` is a live CFArray
    let count = unsafe { CFArrayGetCount(list.0) };
    (0..count).any(|index| {
        // SAFETY: `index` is within the array; the source and its description
        // are borrowed from `info` and `list`, which outlive this closure
        unsafe {
            let source = CFArrayGetValueAtIndex(list.0, index);
            let description = IOPSGetPowerSourceDescription(info.0, source);
            if description.is_null() {
                return false;
            }
            let kind = CFDictionaryGetValue(description, key.0);
            !kind.is_null() && CFEqual(kind, battery.0) != 0
        }
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn bugfix_null_cfstring_is_not_released() {
        assert!(super::CfString::from_created(std::ptr::null()).is_none());
    }

    #[test]
    fn user_activity_is_readable() {
        assert!(super::user_is_active().is_some());
        // tests run on a host that is awake
        assert_eq!(super::fully_awake(), Some(true));
    }

    #[test]
    fn a_desktop_mac_has_no_battery_and_a_laptop_has_one() {
        // `pmset -g batt` names an InternalBattery exactly on a laptop
        let output = std::process::Command::new("pmset")
            .args(["-g", "batt"])
            .output()
            .unwrap();
        let laptop = String::from_utf8_lossy(&output.stdout).contains("InternalBattery");
        assert_eq!(super::has_internal_battery(), laptop);
    }
}
