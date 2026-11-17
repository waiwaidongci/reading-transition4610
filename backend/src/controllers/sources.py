from ..services import sources
class SourcesController:
    list=staticmethod(sources.list_sources)
    save=staticmethod(sources.save_source)
    records=staticmethod(sources.record_page)
    add_records=staticmethod(sources.add_records)
