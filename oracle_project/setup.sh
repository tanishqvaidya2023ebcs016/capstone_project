#!/bin/bash
# =====================================
# Oracle Ubuntu VM — One-time setup
# ====================================
set -e

echo "=== [1/4] Updating system ==="
sudo apt-get update -y

echo "=== [2/4] Installing Python and build tools ==="
sudo apt-get install -y \
    python3.11 \
    python3.11-pip \
    python3-pip \
    build-essential \
    curl \
    screen \
    git

echo "=== [3/4] Installing Python packages ==="
pip3 install \
    grpcio==1.76.0 \
    grpcio-tools==1.76.0 \
    requests==2.32.4 \
    beautifulsoup4==4.13.4 \
    urllib3==2.5.0 \
    protobuf==6.31.1

echo "=== [4/4] Installing Tailscale ==="
curl -fsSL https://tailscale.com/install.sh | sh

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. sudo tailscale up"
echo "     → A login URL will appear — open it in your browser"
echo "     → Log in with the SAME Tailscale account as Tanishq and Shivam"
echo ""
echo "  2. After login, check your Tailscale IP:"
echo "     tailscale ip -4"
echo ""
echo "  3. Edit run.sh — set MAC_IP to Tanishq's Tailscale IP"
echo "     nano run.sh"
echo ""
echo "  4. Start the crawler:"
echo "     screen -S crawler"
echo "     bash run.sh"
echo "     (then Ctrl+A, D to detach and keep it running)"

