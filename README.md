# Telegram Reaction Bot

A fast Rust-based bot that automatically reacts with 👍 to messages containing prices above a certain threshold.

## What it does

This bot monitors Telegram chats for messages containing price information and automatically adds a thumbs-up reaction when the price exceeds 38,000. It's designed for high-performance with minimal latency.

## Requirements

- Rust (latest stable)
- TDLib v1.8.x (Telegram Database Library)
- Telegram account credentials

## Installation

1. Install Rust:
   ```
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. Install TDLib:
   - macOS: `brew install tdlib`
   - Linux: Follow the [official guide](https://tdlib.github.io/td/build.html)

3. Clone this repository:
   ```
   git clone https://github.com/hussainn7/RustScript.git
   cd RustScript
   ```

4. Build the project:
   ```
   cargo build --release
   ```

## Running

1. Set the TDLib path (if not in standard location):
   ```
   export TDLIB_PATH=/path/to/libtdjson.dylib
   ```

2. Run the bot:
   ```
   cargo run --release
   ```

3. Follow the authentication prompts to log in to your Telegram account.

## Configuration

- `MIN_AMOUNT`: Minimum price threshold (default: 38000)
- `REACTION_EMOJI`: Reaction emoji to use (default: 👍)

!! WAS TESTED on Linux and MacOS !!
