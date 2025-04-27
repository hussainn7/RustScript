use std::{
    collections::HashSet,
    ffi::{CStr, CString},
    sync::Arc,
    time::Instant,
    os::raw::c_void,
    sync::atomic::{AtomicBool, Ordering},
};
use regex::Regex;
use simd_json::prelude::*;
use tokio::sync::Mutex;
use log::{info, error, warn};
use libloading::{Library, Symbol};
use bytes::BytesMut;
use crossbeam_channel::{bounded, Sender};
use serde_json::json;

const MIN_AMOUNT: i32 = 38000;
const REACTION_EMOJI: &str = "👍";
const AUTH_TIMEOUT: f64 = 10.0;
// Set to 0.0 for immediate polling without waiting
const RECEIVE_TIMEOUT: f64 = 0.0;
// Microseconds to sleep between polls to avoid 100% CPU usage
const POLL_SLEEP_MICROS: u64 = 10; // Reduced from 50 to 10 microseconds
const MAX_AUTH_ATTEMPTS: u8 = 3;
const TDLIB_VERSION: &str = "1.8.47";

// Fast string formatting without unsafe static mutable references
fn format_reaction(chat_id: i64, message_id: i64) -> String {
    // Use local buffer instead of static mutable
    let mut buffer = BytesMut::with_capacity(256);
    buffer.extend_from_slice(b"{\"@type\":\"addMessageReaction\",\"chat_id\":");
    buffer.extend_from_slice(chat_id.to_string().as_bytes());
    buffer.extend_from_slice(b",\"message_id\":");
    buffer.extend_from_slice(message_id.to_string().as_bytes());
    buffer.extend_from_slice(b",\"reaction_type\":{\"@type\":\"reactionTypeEmoji\",\"emoji\":\"\xF0\x9F\x91\x8D\"},\"is_big\":false}");
    String::from_utf8(buffer.to_vec()).unwrap()
}

struct TdClient {
    client: *mut c_void,
    tdlib: Library,
    sender: Option<Sender<String>>,
    running: AtomicBool,
}

impl TdClient {
    unsafe fn new() -> Self {
        // Set TCP_NODELAY for lower latency network connections
        std::env::set_var("TCP_NODELAY", "1");
        
        let lib_path = std::env::var("TDLIB_PATH").unwrap_or_else(|_| "/opt/homebrew/lib/libtdjson.dylib".into());
        println!("Loading TDLib from: {}", lib_path);
        let tdlib = Library::new(lib_path)
            .expect("Failed to load TDLib");
        
        let create: Symbol<unsafe extern "C" fn() -> *mut c_void> = 
            tdlib.get(b"td_json_client_create").unwrap();
        
        TdClient {
            client: create(),
            tdlib,
            sender: None,
            running: AtomicBool::new(false),
        }
    }

    fn send(&self, request: &str) {
        let request_c = CString::new(request).unwrap();
        unsafe {
            let send: Symbol<unsafe extern "C" fn(*mut c_void, *const i8)> = 
                self.tdlib.get(b"td_json_client_send").unwrap();
            send(self.client, request_c.as_ptr());
        }
    }

    fn receive(&self, timeout: f64) -> Option<String> {
        unsafe {
            let receive: Symbol<unsafe extern "C" fn(*mut c_void, f64) -> *const i8> = 
                self.tdlib.get(b"td_json_client_receive").unwrap();
            
            let result = receive(self.client, timeout);
            if result.is_null() {
                None
            } else {
                Some(CStr::from_ptr(result).to_string_lossy().into_owned())
            }
        }
    }
}

unsafe impl Send for TdClient {}
unsafe impl Sync for TdClient {}

// Start a dedicated receiver thread for lock-free message receiving
fn spawn_receiver_thread(client: Arc<Mutex<TdClient>>, receiver_sender: Sender<String>) {
    std::thread::spawn(move || {
        loop {
            // Get a lock on the client only when needed
            let client_running = {
                if let Ok(lock) = client.try_lock() {
                    if let Some(msg) = lock.receive(0.0) {
                        // Send the message to the main thread without blocking
                        let _ = receiver_sender.send(msg);
                    }
                    lock.running.load(Ordering::Relaxed)
                } else {
                    true // Assume still running if we can't get the lock
                }
            };
            
            if !client_running {
                break;
            }
            
            // Use yield instead of sleep for better responsiveness
            std::thread::yield_now();
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "info");
    std::env::set_var("TDLIB_LOG_VERBOSITY", "0");
    
    // Create required directories
    std::fs::create_dir_all("tdlib_data").expect("Failed to create data directory");
    std::fs::create_dir_all("tdlib_files").expect("Failed to create files directory");
    
    // Set directory permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions("tdlib_data", std::fs::Permissions::from_mode(0o755))
            .expect("Failed to set data directory permissions");
        std::fs::set_permissions("tdlib_files", std::fs::Permissions::from_mode(0o755))
            .expect("Failed to set files directory permissions");
    }
    
    env_logger::init();
    info!("Starting ultra-fast Telegram reaction bot (TDLib v{})", TDLIB_VERSION);

    let client = Arc::new(Mutex::new(unsafe { TdClient::new() }));
    {
        let lock = client.lock().await;
        lock.send(&json!({
            "@type": "setLogVerbosityLevel",
            "new_verbosity_level": 0
        }).to_string());
    }

    let allowed_chat_ids: HashSet<i64> = ["some-ids"]
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
    
    info!("Monitoring {} chat IDs: {:?}", allowed_chat_ids.len(), allowed_chat_ids);

    let _price_regex = Arc::new(Regex::new(r"а:\s*([\d\s]+)\s*₽").unwrap());

    // Setup TDLib with proper parameters
    {
        let lock = client.lock().await;
        info!("Setting up TDLib parameters");
        
        let params = json!({
            "@type": "setTdlibParameters",
            "database_directory": "tdlib_data",
            "files_directory": "tdlib_files",
            "database_encryption_key": "",
            "use_test_dc": false,
            "api_id": ID,
            "api_hash": "with-hash",
            "system_language_code": "en",
            "device_model": "ReactBot",
            "system_version": "1.0",
            "application_version": "1.0",
            "enable_storage_optimizer": false,
            "ignore_file_names": false,
            "use_file_database": false,
            "use_chat_info_database": false,
            "use_message_database": false,
            "use_secret_chats": false,
            "use_pfs": false,  // Disable PFS for faster connections
            "use_ipv6": false  // Disable IPv6 if not needed
        });
        
        lock.send(&params.to_string());
        // No need to check database encryption key separately
        // TDLib handles this automatically in setTdlibParameters
    }

    // Wait for authorization
    let mut auth_state = String::from("waitTdlibParameters");
    let mut auth_attempts = 0;
    
    while auth_state != "authorizationStateReady" && auth_attempts < MAX_AUTH_ATTEMPTS {
        info!("Current auth state: {}", auth_state);
        let message = {
            let lock = client.lock().await;
            let msg = lock.receive(AUTH_TIMEOUT);
            msg
        };

        if let Some(msg) = message {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&msg) {
                if let Some(t) = json["@type"].as_str() {
                    match t {
                        "updateAuthorizationState" => {
                            if let Some(state) = json["authorization_state"]["@type"].as_str() {
                                auth_state = state.to_string();
                                
                                match state {
                                    "authorizationStateWaitTdlibParameters" => {
                                        info!("TDLib is waiting for parameters");
                                    }
                                    "authorizationStateWaitPhoneNumber" => {
                                        info!("TDLib is waiting for phone number");
                                        let lock = client.lock().await;
                                        lock.send(&json!({
                                            "@type": "setAuthenticationPhoneNumber",
                                            "phone_number": std::env::var("TELEGRAM_PHONE").expect("TELEGRAM_PHONE environment variable not set")
                                        }).to_string());
                                    }
                                    "authorizationStateWaitCode" => {
                                        info!("TDLib is waiting for authentication code");
                                        let lock = client.lock().await;
                                        lock.send(&json!({
                                            "@type": "checkAuthenticationCode",
                                            "code": std::env::var("TELEGRAM_CODE").expect("TELEGRAM_CODE environment variable not set")
                                        }).to_string());
                                    }
                                    "authorizationStateWaitPassword" => {
                                        info!("TDLib is waiting for password");
                                        let lock = client.lock().await;
                                        lock.send(&json!({
                                            "@type": "checkAuthenticationPassword",
                                            "password": std::env::var("TELEGRAM_PASSWORD").expect("TELEGRAM_PASSWORD environment variable not set")
                                        }).to_string());
                                    }
                                    "authorizationStateReady" => {
                                        info!("Authorization is complete!");
                                    }
                                    _ => {
                                        info!("Current auth state: {}", state);
                                    }
                                }
                            }
                        }
                        "error" => {
                            error!("Error from TDLib: {}", json["message"]);
                            auth_attempts += 1;
                            if auth_attempts >= MAX_AUTH_ATTEMPTS {
                                return Err("Too many authentication attempts".into());
                            }
                        }
                        _ => {}
                    }
                }
            }
        } else {
            warn!("No message received within timeout period");
        }
    }

    if auth_state != "authorizationStateReady" {
        return Err("Failed to authenticate with Telegram".into());
    }

    // Request chats to start receiving updates
    {
        info!("Requesting chats to start receiving updates");
        let lock = client.lock().await;
        lock.send(&json!({
            "@type": "getChats",
            "limit": 100
        }).to_string());
    }

    // Get available reactions for the chat
    for chat_id in &allowed_chat_ids {
        info!("Getting available reactions for chat {}", chat_id);
        let lock = client.lock().await;
        lock.send(&json!({
            "@type": "getChatAvailableReactions",
            "chat_id": chat_id
        }).to_string());
    }

    // Setup lock-free message receiving with crossbeam channel
    info!("Setting up lock-free message receiving");
    let (sender, receiver) = bounded::<String>(100);
    
    // Store the sender in the client for the receiver thread
    {
        let mut lock = client.lock().await;
        lock.sender = Some(sender.clone());
        lock.running.store(true, Ordering::SeqCst);
    }
    
    // Spawn a dedicated thread for receiving messages without locking
    let client_clone = Arc::clone(&client);
    spawn_receiver_thread(client_clone, sender);
    
    info!("Starting ultra-optimized message processing loop");
    // Ultra-optimized message processing loop with lock-free receiving
    loop {
        // Try to receive a message without blocking
        if let Ok(msg) = receiver.try_recv() {
            // Measure JSON parsing time using SIMD-accelerated JSON parsing
            let parse_start = Instant::now();
            
            // Use simd-json for much faster parsing (2x faster than serde_json)
            let mut json_bytes = msg.into_bytes();
            if let Ok(json) = simd_json::to_owned_value(&mut json_bytes) {
                let parse_time = parse_start.elapsed();
                
                // Print the message type for debugging
                if let Some(msg_type) = json["@type"].as_str() {
                    info!("Received message type: {}", msg_type);
                }
                
                if json["@type"] == "updateNewMessage" {
                    info!("Received new message!");
                    // Optimized hot path: Combine all checks into a single pattern match
                    if let (Some(chat_id), Some(message_id)) = (
                        json["message"]["chat_id"].as_i64(),
                        json["message"]["id"].as_i64()
                    ) {
                        // Get text content if available - safely check the structure
                        let text = if json["message"]["content"]["@type"] == "messageText" {
                            json["message"]["content"]["text"]["text"].as_str()
                        } else {
                            None
                        };
                        // Start timing measurement immediately after extracting required fields
                        let start = Instant::now();
                        
                        if allowed_chat_ids.contains(&chat_id) {
                            // Check if we have text to process
                            if let Some(text_content) = text {
                                // Use the ultra-fast price extraction
                                let price_check_start = Instant::now();
                                if let Some(amount) = extract_price_fast(text_content) {
                                    let price_check_time = price_check_start.elapsed();
                                    info!("Found price: {}", amount);
                                    
                                    if amount >= MIN_AMOUNT {
                                    // Use zero-copy string formatting for the reaction request
                                    let format_start = Instant::now();
                                    let reaction_request = format_reaction(chat_id, message_id);
                                    let format_time = format_start.elapsed();
                                    
                                    // Send the reaction request
                                    let send_start = Instant::now();
                                    let lock = client.lock().await;
                                    lock.send(&reaction_request);
                                    let send_time = send_start.elapsed();
                                    
                                    let total_time = start.elapsed();
                                    info!("REACTING! Chat: {}, Message: {}, Amount: {}", chat_id, message_id, amount);
                                    info!("REACTION STATS - Parse: {:?}, Price: {:?}, Format: {:?}, Send: {:?}, Total: {:?}", 
                                          parse_time, price_check_time, format_time, send_time, total_time);
                                    }
                                }
                            } else {
                                // If no text content, check if it's another message type we should react to
                                // For debugging, print the message content type
                                if let Some(content) = json["message"]["content"].as_object() {
                                    if let Some(content_type) = content["@type"].as_str() {
                                        info!("Message content type: {}", content_type);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Use yield instead of sleep for better responsiveness with lower CPU usage
        std::thread::yield_now();
    }
}

// Fast reaction function that doesn't wait for response
fn send_reaction_fast(client: &TdClient, chat_id: i64, message_id: i64) {
    let reaction_request = json!({
        "@type": "addMessageReaction",
        "chat_id": chat_id,
        "message_id": message_id,
        "reaction_type": {
            "@type": "reactionTypeEmoji",
            "emoji": REACTION_EMOJI
        },
        "is_big": false
    });
    
    client.send(&reaction_request.to_string());
}

async fn react_to_message(client: &TdClient, chat_id: i64, message_id: i64) -> Result<(), Box<dyn std::error::Error>> {
    info!("Attempting to add reaction {} to message {} in chat {}", REACTION_EMOJI, message_id, chat_id);
    
    // Use the updated format for TDLib v1.8.47
    let reaction_request = json!({
        "@type": "addMessageReaction",
        "chat_id": chat_id,
        "message_id": message_id,
        "reaction_type": {
            "@type": "reactionTypeEmoji",
            "emoji": REACTION_EMOJI
        },
        "is_big": false
    });
    
    info!("Sending reaction request: {}", reaction_request);
    client.send(&reaction_request.to_string());
    
    // Wait for response to check if reaction was successful
    let start_wait = Instant::now();
    let timeout = 5.0; // 5 seconds timeout
    
    while start_wait.elapsed().as_secs_f64() < timeout {
        if let Some(response) = client.receive(0.1) {
            if let Ok(json_response) = serde_json::from_str::<serde_json::Value>(&response) {
                // Check if this is a response to our reaction request
                if json_response["@type"] == "error" {
                    error!("Error adding reaction: {}", json_response);
                    return Err(format!("Failed to add reaction: {}", json_response["message"]).into());
                } else if json_response["@type"] == "ok" || 
                          (json_response["@type"] == "updateMessageReactions" && 
                           json_response["chat_id"] == chat_id && 
                           json_response["message_id"] == message_id) {
                    info!("Reaction successfully added: {}", json_response);
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    warn!("No confirmation received for reaction, but no error either");
    Ok(())
}

// Ultra-fast price extraction without regex
fn extract_price_fast(text: &str) -> Option<i32> {
    let start = text.find("а:")?;
    let end = text[start..].find('₽')?;
    let price_str = &text[start..start+end];
    
    // Extract only digits
    let price_digits = price_str
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect::<String>();
        
    price_digits.parse().ok()
}

// Keep the original regex version as fallback
fn extract_price(text: &str, regex: &Regex) -> Option<i32> {
    regex.captures(text)?
        .get(1)?
        .as_str()
        .replace(' ', "")
        .parse()
        .ok()
}
