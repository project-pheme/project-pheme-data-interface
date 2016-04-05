FROM alpine:3.3

RUN apk add --update python python-dev py-pip gcc musl-dev linux-headers && \
    rm -rf /var/cache/apk/*

WORKDIR /var/app
COPY requirements.txt /var/app/
RUN pip install -r requirements.txt

COPY . /var/app/

EXPOSE 8888

ENTRYPOINT ["/bin/sh"]
CMD [ "-c", "cd ./store ; python ./api.py --logging=info" ]
