import app from './app';
import { config } from './config';
import { startPeriodAutoLockJob } from './jobs/periodAutoLock.job';
import { startReportMailerJob } from './jobs/reportMailer.job';

app.listen(config.port, () => {
  console.log(`Warehouse API running on http://localhost:${config.port}`);
    // startPeriodAutoLockJob();
    startReportMailerJob()
});
