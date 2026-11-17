from sqlalchemy import Column,Integer,String,JSON,Text,ForeignKey
from ..config.database import Base
from ..config.settings import CONFIG
class Run(Base):
    __tablename__=CONFIG['run_table']
    id=Column(Integer,primary_key=True)
    source_id=Column(Integer,ForeignKey(CONFIG['source_table']+'.id'),nullable=False)
    name=Column(String(160),nullable=False)
    state=Column(String(20),default='queued')
    phase=Column(String(24),default='')
    progress=Column(Integer,default=0)
    frozen_count=Column(Integer,default=0)
    processed_count=Column(Integer,default=0)
    peak_memory_kb=Column(Integer,default=0)
    intake_peak_kb=Column(Integer,default=0)
    compute_peak_kb=Column(Integer,default=0)
    attempts=Column(Integer,default=0)
    parameters=Column(JSON,nullable=False)
    snapshot=Column(JSON,nullable=False)
    result=Column(JSON,nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
