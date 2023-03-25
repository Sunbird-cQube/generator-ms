FROM python:3.7

WORKDIR /python_app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

#ENTRYPOINT ["tail", "-f", "/dev/null"]
WORKDIR /python_app/static_processor_group
CMD [ "python3", "add_nifi_template.py"]
