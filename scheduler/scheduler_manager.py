from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.triggers.cron import CronTrigger
from utils.segment_etl import run_segment_cleaning
from utils.logger import app_logger
from service.price_tag import  generate_and_upload_price_tags_for_all_orgs
from typing import Optional
from service import get_db
from utils.app_config import app_config

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

            # self.scheduler.add_job(
            #     self._run_price_tag_generation,
            #     CronTrigger(hour=8, minute=33),
            #     id='price_tag_generation_job',
            #     replace_existing=True,
            #     name='Daily price tag generation at xx:xx'
            # )
            # app_logger.info("Price tag generation job scheduled for 11:40 daily.")

            tag_configs = app_config.dict_config.get('SFTP_CONFIG', {}).get('TAG', [])

            if not tag_configs:
                app_logger.warning("No TAG configuration found in SFTP_CONFIG")
            else:
                for idx, org_config in enumerate(tag_configs):
                    org_id = org_config.get('ORG_ID')
                    tag_schedule = org_config.get('TAG_SCHEDULE', '23:59')

                    if not org_id:
                        app_logger.warning(f"Organization {idx + 1} missing ORG_ID, skipping")
                        continue

                    try:
                        hour, minute = map(int, tag_schedule.split(':'))
                    except (ValueError, AttributeError) as e:
                        app_logger.error(f"Invalid TAG_SCHEDULE format '{tag_schedule}' for org {org_id}: {e}")
                        hour, minute = 23, 59

                    job_id = f'price_tag_generation_job_org_{org_id}'
                    job_name = f'Price tag generation for org {org_id} at {hour:02d}:{minute:02d}'

                    self.scheduler.add_job(
                        self._run_price_tag_generation,
                        CronTrigger(hour=hour, minute=minute),
                        id=job_id,
                        replace_existing=True,
                        name=job_name
                    )
                    app_logger.info(f"Price tag generation job scheduled: {job_name} (ID: {job_id})")

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
