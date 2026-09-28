import pandas as pd
import matplotlib.pyplot as plt

PLOT_FILE_QUEUE_LENGTH = "measurements/queue-length-plot.png"
PLOT_FILE_AVG_QUEUE_TIME = "measurements/avg-queue-time-plot.png"
PLOT_FILE_WAITING_S2 = "measurements/waiting_s2_plot.png"

df1 = pd.read_csv('measurements/baseline/normal/run2/gateway_baseline_metrics.csv')
df2 = pd.read_csv('measurements/baseline/brownout/run1/gateway_baseline_metrics.csv')

max_time = 98
df1_filtered = df1[df1['t_s'] <= max_time]
df2_filtered = df2[df2['t_s'] <= max_time]

plt.figure(figsize=(12, 6))

plt.plot(df1_filtered['t_s'], df1_filtered['queue_len'], label='normal run', color='blue', linewidth=1.5)
plt.plot(df2_filtered['t_s'], df2_filtered['queue_len'], label='brownout run', color='orange', linewidth=1.5)


plt.xlabel('time (s)', fontsize=12)
plt.ylabel('queue length', fontsize=12)
plt.title(f'Queue length - comparison', fontsize=14)
plt.legend(fontsize=11)
plt.grid(True, linestyle=':', alpha=0.7)


plt.tight_layout()
plt.savefig(PLOT_FILE_QUEUE_LENGTH)
plt.show()

plt.close()

plt.figure(figsize=(12, 6))

plt.plot(df1_filtered['t_s'], df1_filtered['avg_queue_wait_ms'], label='normal run', color='blue', linewidth=1.5)
plt.plot(df2_filtered['t_s'], df2_filtered['avg_queue_wait_ms'], label='brownout run', color='orange', linewidth=1.5)


plt.xlabel('time (s)', fontsize=12)
plt.ylabel('Average wait time in queue (ms)', fontsize=12)
plt.title(f'Average wait time in queue - comparison', fontsize=14)
plt.legend(fontsize=11)
plt.grid(True, linestyle=':', alpha=0.7)


plt.tight_layout()
plt.savefig(PLOT_FILE_AVG_QUEUE_TIME)
plt.show()
plt.close()

plt.figure(figsize=(12, 6))

plt.plot(df1_filtered['t_s'], df1_filtered['waiting_s2'], label='normal run', color='blue', linewidth=1.5)
plt.plot(df2_filtered['t_s'], df2_filtered['waiting_s2'], label='brownout run', color='orange', linewidth=1.5)


plt.xlabel('time (s)', fontsize=12)
plt.ylabel('Amount of workers waiting for Server 2', fontsize=12)
plt.title(f'Amount of workers waiting for Server 2 - comparison', fontsize=14)
plt.legend(fontsize=11)
plt.grid(True, linestyle=':', alpha=0.7)


plt.tight_layout()
plt.savefig(PLOT_FILE_WAITING_S2)
plt.show()
plt.close()

