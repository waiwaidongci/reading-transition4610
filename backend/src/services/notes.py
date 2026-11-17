from fastapi import HTTPException
from sqlalchemy import select
from ..models.note import Note
from .runs import get_run
from .audit import log
NOTE_LIST_COLUMNS=(Note.id,Note.run_id,Note.title,Note.body,Note.state,Note.author,Note.approved_by)
def list_notes(db):
    # 列表不读取 frozen_result：大文集研究的结果快照不应整批进入内存或回传浏览器。
    return [dict(record._mapping) for record in db.execute(select(*NOTE_LIST_COLUMNS).order_by(Note.id.desc()))]
def create_note(db,user,data):
    r=get_run(db,data.run_id)
    if r.state!='completed':raise HTTPException(409,'只可为已完成结果提交说明')
    n=Note(run_id=r.id,title=data.title,body=data.body,state='draft',frozen_result=r.result,author=user.email)
    db.add(n);db.flush();log(db,user,'note.created',n.id,n.title);db.commit()
    return {key:getattr(n,key) for key in ('id','run_id','title','body','state','author','approved_by')}
def approve_note(db,user,id):
    n=db.get(Note,id)
    if not n:raise HTTPException(404,'未找到说明')
    if n.state!='draft':raise HTTPException(409,'该说明已经批准')
    n.state='approved';n.approved_by=user.email;log(db,user,'note.approved',n.id,n.title);db.commit()
    return {key:getattr(n,key) for key in ('id','run_id','title','body','state','author','approved_by')}
