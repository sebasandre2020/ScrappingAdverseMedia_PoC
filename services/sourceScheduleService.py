from repositories.sourceRepository import SourceRepository
from repositories.resultRepository import ResultRepository
from typing import List
from db_models.locals.sourceScheduleModel import SourceSchedule

class SourceScheduleService:
    __source_repository = SourceRepository()
    __result_repository = ResultRepository()

    def updateSources(self, source_schedules: List[SourceSchedule]):
        sources_to_update = SourceScheduleService.__source_repository.get_sources_to_update(source_schedules)
        source_codes_to_update = [source_to_update.sourceCode for  source_to_update in sources_to_update]
        entity_results = SourceScheduleService.__result_repository.get_entity_results_by_source_codes_array(source_codes_to_update)
        return entity_results
