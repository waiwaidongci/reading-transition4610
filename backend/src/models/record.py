from sqlalchemy import Column,Integer,String,JSON,ForeignKey,UniqueConstraint,Index
from ..config.database import Base
from ..config.settings import CONFIG
class Record(Base):
    __tablename__=CONFIG['record_table']
    # (source_id, id) 支持受理阶段的键集游标分批：百万行上避免逐批全表扫描。
    __table_args__=(UniqueConstraint('source_id','reference',name='uq_record_reference'),
                    Index('ix_%s_source_id_id' % CONFIG['record_table'],'source_id','id'))
    id=Column(Integer,primary_key=True)
    source_id=Column(Integer,ForeignKey(CONFIG['source_table']+'.id'),nullable=False)
    reference=Column(String(120),nullable=False)
    values=Column(JSON,nullable=False)
