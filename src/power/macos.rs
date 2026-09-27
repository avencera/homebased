//! IOKit power assertion that blocks idle system sleep.

use std::ffi::{CStr, c_char, c_void};

type CfStringRef = *const c_void;
type IoPmAssertionId = u32;
type IoReturn = i32;

const K_CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
const K_IOPM_ASSERTION_LEVEL_ON: u32 = 255;
const K_IO_RETURN_SUCCESS: IoReturn = 0;
// IOReturn codes are 32-bit patterns; the wrap to i32 is intended
const K_IO_RETURN_BAD_ARGUMENT: IoReturn = 0xE000_02C2_u32 as IoReturn;
// the assertion `caffeinate -i` takes; any user may create it
const PREVENT_USER_IDLE_SYSTEM_SLEEP: &CStr = c"PreventUserIdleSystemSleep";

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    fn CFStringCreateWithCString(
        alloc: *const c_void,
        c_str: *const c_char,
        encoding: u32,
    ) -> CfStringRef;
    fn CFRelease(cf: *const c_void);
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
        (!raw.is_null()).then_some(Self(raw))
    }
}

impl Drop for CfString {
    fn drop(&mut self) {
        // SAFETY: `self.0` came from a Create function and is released once
        unsafe { CFRelease(self.0) };
    }
}

/// A held `PreventUserIdleSystemSleep` assertion, released on drop.
#[derive(Debug)]
pub(super) struct IdleSleepAssertion(IoPmAssertionId);

impl IdleSleepAssertion {
    /// Create the assertion. `name` shows in `pmset -g assertions`. The error
    /// is the `IOReturn` code.
    pub(super) fn create(name: &CStr) -> Result<Self, IoReturn> {
        let assertion_type =
            CfString::new(PREVENT_USER_IDLE_SYSTEM_SLEEP).ok_or(K_IO_RETURN_BAD_ARGUMENT)?;
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

impl Drop for IdleSleepAssertion {
    fn drop(&mut self) {
        // SAFETY: the id came from a successful create and is released once;
        // a failed release leaves nothing to clean up, and process exit
        // releases it anyway
        unsafe { IOPMAssertionRelease(self.0) };
    }
}
