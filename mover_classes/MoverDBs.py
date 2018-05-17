import os
from peewee import *
from mover_classes.Item import PItem
from datetime import date, datetime

db = SqliteDatabase('StoPMove.db', pragmas=(('foreign_keys', 'on'),))


class BaseModel(Model):
    class Meta:
        database = db


class ProjectID(BaseModel):
    project_file = TextField(unique=True)
    project_added = DateTimeField()
    project_completed = BooleanField(default=False)


class StoPDB(BaseModel):
    pid = ForeignKeyField(ProjectID)
    s_root = TextField(unique=True)
    p_root = TextField(unique=True)
    s_validated_on = DateTimeField(null=True)
    p_validated_on = DateTimeField(null=True)
    s_removed_on = DateTimeField(null=True)
    completed_move = BooleanField(default=False)


class StoPparts(BaseModel):
    root = ForeignKeyField(StoPDB)
    lvl1 = TextField()
    lvl2 = TextField()
    lvl3 = TextField()
    lvl4 = TextField()
    lvl5 = TextField()
    lvl6 = TextField()


class FakeAccessions(BaseModel):
    root = ForeignKeyField(StoPDB)
    used_accessions = IntegerField()


db.connect()

try:
    db.create_tables([ProjectID, StoPDB, StoPparts, FakeAccessions])
except OperationalError as e:
    pass

if __name__ == '__main__':
    f = r"L:\Intranet\ar\Digital_Services\Inventory\005_A_COMPLETE\S_BAGS_001\S_BAGS_JMG_001_ORIGINAL.tsv"
    p_items = []
    projectId = None

    try:
        projectId = ProjectID(project_file=f, project_added=datetime.now())
        projectId.save()
        print("Starting project: {}".format(f))
    except IntegrityError as e:
        print("Getting project: {}".format(f))
        projectId = ProjectID.get(ProjectID.project_file == f)
        if projectId.project_completed:
            print("This project has been successfully completed")
            exit(1)

    with open(f) as fh:
        for line in fh.readlines():
            s = line.strip().split("\t")
            pi = PItem(s, False)
            print("Adding to database: {}".format(pi.current_location))
            s2p = StoPDB(pid=projectId)
            try:
                s2p.p_root = pi.p_location
                s2p.s_root = pi.current_location
                s2p.save()
            except IntegrityError as e:
                print("{}\t{}".format(e, pi.p_location))
                exit(-1)
            if os.path.exists(pi.p_location):
                s2p.completed_move = True
                s2p.save()
            parts = StoPparts(root=s2p)
            parts.lvl1 = pi.record_status
            parts.lvl2 = pi.collection_type
            parts.lvl3 = pi.record_group
            parts.lvl4 = pi.series
            parts.lvl5 = pi.item
            parts.lvl6 = pi.accession_number
            parts.save()


    projectId.project_completed = True
    projectId.save()
