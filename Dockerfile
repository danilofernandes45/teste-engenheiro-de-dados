FROM ubuntu:18.04

RUN apt update -y  &&  apt upgrade -y && apt-get update 
RUN apt install -y curl python3.7 git python3-pip openjdk-8-jdk unixodbc-dev

# # Add SQL Server ODBC Driver 17 for Ubuntu 18.04
# RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# RUN apt-get update
# RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql17
# RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated mssql-tools
# RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
# RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc


RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN echo "deb [arch=amd64] https://packages.microsoft.com/ubuntu/18.04/prod bionic main" | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

RUN pip3 install pyodbc pandas numpy

EXPOSE 7000

WORKDIR /res

COPY import_data/acess.py /res/
# COPY data/microdados_enem_2020/DADOS/MICRODADOS_ENEM_2020_sample.csv /res/
COPY import_data/MICRODADOS_ENEM_2020_sample.csv /res/
COPY import_data/MICRODADOS_ENEM_2020.csv /res/
COPY import_data/test.py /res/
CMD ["python3","./acess.py"]