from pydantic import BaseModel, computed_field


class HandlerConfig(BaseModel):
    name: str
    task_type: str
    version: str
    description: str = ''

    import_path: str  # TODO rename to interface_function_import_path
    interface_endpoint_url: str = ''

    handler_repo: str = ''
    db_loader_script_path: str = ''
    service_launcher_script_path: str = ''

    @computed_field(return_type=str)
    @property
    def handler_id(self):
        return f'{self.task_type}:{self.version}'
