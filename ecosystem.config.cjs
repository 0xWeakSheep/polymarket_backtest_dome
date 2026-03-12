module.exports = {
  apps: [
    {
      name: "btc-5m-arrival",
      cwd: "/Users/caoxiangrui/Desktop/external/polymarket_backtest",
      script: "./scripts/run_btc_5m_arrival.sh",
      interpreter: "bash",
      autorestart: true,
      max_restarts: 20,
      restart_delay: 5000,
      kill_timeout: 10000,
      time: true,
      merge_logs: true,
      out_file: "./logs/btc_5m_arrival.out.log",
      error_file: "./logs/btc_5m_arrival.err.log",
      env: {
        PYTHONUNBUFFERED: "1",
      },
    },
    {
      name: "tail-reversal-095",
      cwd: "/Users/caoxiangrui/Desktop/external/polymarket_backtest",
      script: "./scripts/run_tail_reversal.sh",
      interpreter: "bash",
      autorestart: true,
      max_restarts: 20,
      restart_delay: 5000,
      kill_timeout: 10000,
      time: true,
      merge_logs: true,
      out_file: "./logs/tail_reversal_095.out.log",
      error_file: "./logs/tail_reversal_095.err.log",
      env: {
        PYTHONUNBUFFERED: "1",
      },
    },
  ],
};
