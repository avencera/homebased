//! Usage in Claude Code's `stream-json` output
//!
//! Each run ends with a `result` event that totals its usage per model. A run
//! stopped before that leaves only `assistant` events. Their usage repeats on
//! every content block of a message, and their output tokens are the count
//! when the message started, so they are kept per message id and the task is
//! marked incomplete

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Deserialize;

use super::{ModelUsage, TaskUsage, Tokens, Usd};

// a cheap filter before parsing; an event's own `type` key is never escaped,
// while the same text inside a tool result is
const RESULT_MARKER: &[u8] = br#""type":"result""#;
const ASSISTANT_MARKER: &[u8] = br#""type":"assistant""#;

/// Model name for an assistant message that names none
const UNKNOWN_MODEL: &str = "unknown";

/// Usage in the output file at `path`, empty when it holds none or is unreadable
pub(super) fn read_output(path: &Path) -> TaskUsage {
    let Ok(file) = File::open(path) else {
        return TaskUsage::default();
    };
    let mut reader = BufReader::new(file);
    let mut scan = Scan::default();
    let mut line = Vec::new();
    while reader
        .read_until(b'\n', &mut line)
        .is_ok_and(|read| read > 0)
    {
        scan.line(&line);
        line.clear();
    }

    scan.finish()
}

#[derive(Default)]
struct Scan {
    models: BTreeMap<String, Tokens>,
    turns: u64,
    /// Assistant messages since the last `result`, by message id
    pending: HashMap<String, (String, Tokens)>,
}

impl Scan {
    fn line(&mut self, line: &[u8]) {
        if !contains(line, RESULT_MARKER) && !contains(line, ASSISTANT_MARKER) {
            return;
        }
        let Ok(event) = serde_json::from_slice::<Event>(line) else {
            return;
        };

        match event {
            Event::Result {
                num_turns,
                model_usage,
            } => {
                for (model, usage) in model_usage {
                    *self.models.entry(model).or_default() += Tokens::from(usage);
                }
                self.turns += num_turns;
                // the result counts every message of its run
                self.pending.clear();
            }
            Event::Assistant { message } => {
                let (Some(id), Some(usage)) = (message.id, message.usage) else {
                    return;
                };
                let model = message.model.unwrap_or_else(|| UNKNOWN_MODEL.into());
                self.pending.insert(id, (model, usage.into()));
            }
            Event::Other => {}
        }
    }

    fn finish(mut self) -> TaskUsage {
        let complete = self.pending.is_empty();
        self.turns += self.pending.len() as u64;
        for (model, tokens) in self.pending.into_values() {
            *self.models.entry(model).or_default() += tokens;
        }

        let models = self
            .models
            .into_iter()
            .map(|(model, tokens)| ModelUsage { model, tokens })
            .collect();
        TaskUsage::new(complete, self.turns, models)
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event {
    Result {
        #[serde(default)]
        num_turns: u64,
        #[serde(default, rename = "modelUsage")]
        model_usage: BTreeMap<String, ResultModelUsage>,
    },
    Assistant {
        message: Message,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultModelUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    #[serde(default, rename = "costUSD")]
    cost_usd: f64,
}

impl From<ResultModelUsage> for Tokens {
    fn from(usage: ResultModelUsage) -> Self {
        Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cache_read_tokens: usage.cache_read_input_tokens,
            cache_write_tokens: usage.cache_creation_input_tokens,
            cost_usd: Usd(usage.cost_usd),
        }
    }
}

#[derive(Deserialize)]
struct Message {
    id: Option<String>,
    model: Option<String>,
    usage: Option<MessageUsage>,
}

#[derive(Deserialize)]
struct MessageUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
}

impl From<MessageUsage> for Tokens {
    fn from(usage: MessageUsage) -> Self {
        Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cache_read_tokens: usage.cache_read_input_tokens,
            cache_write_tokens: usage.cache_creation_input_tokens,
            cost_usd: Usd(0.0),
        }
    }
}
