git pull origin main 
 
cargo build --release --locked

sudo install -m 0755 target/release/meshcore-bot \
  /usr/local/bin/meshcore-bot-rust.new
sudo mv /usr/local/bin/meshcore-bot-rust.new \
  /usr/local/bin/meshcore-bot-rust

sudo systemctl restart meshcore-bot-rust
systemctl --no-pager status meshcore-bot-rust
