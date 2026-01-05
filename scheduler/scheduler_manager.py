from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.triggers.cron import CronTrigger
from utils.segment_etl import run_segment_cleaning
from utils.logger import app_logger

from typing import Optional

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
