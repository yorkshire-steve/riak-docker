FROM python:3.8-alpine

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src ./

CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-p", "*_test.py"]
