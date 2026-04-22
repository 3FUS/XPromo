from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.triggers.cron import CronTrigger
from utils.segment_etl import run_segment_cleaning
from utils.logger import app_logger
from service.price_tag import  generate_and_upload_price_tags_for_all_orgs
from typing import Optional
from service import get_db

class SchedulerManager:
    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None

    def init_scheduler(self):
        """初始化调度器"""
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': AsyncIOExecutor()
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }

        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults
        )

    async def start_scheduler(self):
        """启动调度器"""
        if self.scheduler:
            self.scheduler.start()
            app_logger.info("Scheduler started successfully.")
            self.scheduler.add_job(
                run_segment_cleaning,
                'interval',
                minutes=1,
                id='segment_cleaning_job',
                replace_existing=True
            )

            self.scheduler.add_job(
                self._run_price_tag_generation,
                CronTrigger(hour=23, minute=58),
                id='price_tag_generation_job',
                replace_existing=True,
                name='Daily price tag generation at xx:xx'
            )
            app_logger.info("Price tag generation job scheduled for 11:40 daily.")

            for job in self.scheduler.get_jobs():
                app_logger.info(f"Scheduled job: {job.id}, next run: {job.next_run_time}")

    async def _run_price_tag_generation(self):
        """执行价格标签生成任务"""
        try:
            app_logger.info("Starting price tag generation job...")
            db = next(get_db())
            # result = generate_and_upload_price_tags(db)
            result = generate_and_upload_price_tags_for_all_orgs(db)

            if result['success']:
                app_logger.info(f"Price tag generation completed: {result.get('message')}")
                app_logger.info(
                    f"File: {result.get('filename')}, Records: {result.get('record_count')}, Uploaded: {result.get('uploaded')}")
            else:
                app_logger.error(f"Price tag generation failed: {result.get('message')}")
        except Exception as e:
            app_logger.error(f"Error in price tag generation job: {str(e)}", exc_info=True)
        finally:
            db.close()

    async def shutdown_scheduler(self):
        """关闭调度器"""
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown()
            app_logger.info("Scheduler shutdown successfully.")

    def add_job(self, func, trigger, **kwargs):
        """添加调度任务"""
        if self.scheduler:
            return self.scheduler.add_job(func, trigger, **kwargs)

    def remove_job(self, job_id: str):
        """移除调度任务"""
        if self.scheduler:
            self.scheduler.remove_job(job_id)


scheduler_manager = SchedulerManager()
