from airflow.models.baseoperator import BaseOperator

class HelloWordOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
    
    def execute(self, context):
        message = f"Olá {self.name}"
        print(message)
        return message