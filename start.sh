#!/bin/bash
# Generate .streamlit/secrets.toml from environment variables for Railway deployment
mkdir -p .streamlit

cat > .streamlit/secrets.toml << EOF
GOOGLE_API_KEY = "${GOOGLE_API_KEY:-}"
SUPABASE_DB_URL = "${SUPABASE_DB_URL:-}"
SUPABASE_URL = "${SUPABASE_URL:-}"
SUPABASE_KEY = "${SUPABASE_KEY:-}"
OPENAI_API_KEY = "${OPENAI_API_KEY:-}"
EOF

exec streamlit run app.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true
