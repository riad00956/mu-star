import os
import threading
from flask import Flask, jsonify
from star import main as bot_main

app = Flask(__name__)

@app.route('/')
@app.route('/health')
def health():
    return jsonify(status="alive")

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

if __name__ == "__main__":
    # Start Flask in a daemon thread so it doesn't block the bot
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    # Run the Telegram bot (blocking)
    bot_main()
