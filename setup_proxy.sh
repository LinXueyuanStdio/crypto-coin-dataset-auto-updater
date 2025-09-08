echo "Setting up proxy configuration..."
sudo apt-get update
sudo apt-get install -y tor privoxy
sudo bash -c 'cat >> /etc/privoxy/config << EOL
forward-socks5 / 127.0.0.1:9050 .
listen-address  127.0.0.1:8119
max-client-connections 256
keep-alive-timeout 5
socket-timeout 300
EOL'
sudo service tor start
sudo service privoxy start
curl --proxy http://127.0.0.1:8119 https://api.ipify.org || true
sleep 15  # Allow services to stabilize