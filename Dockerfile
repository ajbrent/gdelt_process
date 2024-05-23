FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt lambda_handler.py ${LAMBDA_TASK_ROOT}/
COPY src/etl.py src/__init__.py ${LAMBDA_TASK_ROOT}/src/

RUN pip install -r requirements.txt

CMD [ "lambda_handler.lambda_handler" ]
