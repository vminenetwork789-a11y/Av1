#!/usr/bin/env bash
set -e

echo "=========================================="
echo "🚀 Starting VPS Update for Matrix 27 Ranks"
echo "=========================================="

# 1. สำรองไฟล์ .env และฐานข้อมูลล่าสุดไว้ก่อน
if [ -f ".env" ]; then
  cp .env .env.backup
fi
if [ -f "prisma/matrix-db.json" ]; then
  cp prisma/matrix-db.json prisma/matrix-db.json.backup
fi

# 2. หากใช้ Git ให้บังคับดึงโค้ดล่าสุดโดยไม่ติดปัญหา Local changes conflict
if [ -d ".git" ]; then
  echo "📥 Fetching latest code from Git..."
  git fetch --all
  BRANCH=$(git rev-parse --abbrev-ref HEAD || echo "main")
  git reset --hard "origin/$BRANCH" || git reset --hard origin/main || git reset --hard origin/master
fi

# คืนค่าไฟล์ .env
if [ -f ".env.backup" ]; then
  cp .env.backup .env
fi

# 3. ติดตั้ง Dependencies
echo "📦 Installing dependencies..."
npm install --legacy-peer-deps || npm install

# 4. สั่ง Build (ตั้งค่าเพิ่ม RAM ให้ Node ป้องกัน Out of Memory บน VPS)
echo "🛠️ Building Frontend (dist/) & Backend (server.js)..."
NODE_OPTIONS="--max-old-space-size=2048" npm run build:hostinger || {
  echo "⚠️ Build skipped or failed on VPS — using pre-built dist/ and server.js included in project."
}

# 5. รีสตาร์ท PM2
echo "🔄 Restarting PM2 process..."
if command -v pm2 >/dev/null 2>&1; then
  pm2 restart all --update-env || NODE_ENV=production PORT=3000 pm2 start server.js --name "matrix-server"
  pm2 save
else
  echo "⚠️ PM2 not found. Run: NODE_ENV=production PORT=3000 node server.js"
fi

# 6. สั่ง Sync ข้อมูลจาก Smart Contract ใหม่
echo "⏳ Waiting 3 seconds for server to start..."
sleep 3
curl -s -X POST http://localhost:3000/api/db/sync >/dev/null 2>&1 || true

echo "=========================================="
echo "✅ VPS Update Completed Successfully!"
echo "=========================================="
