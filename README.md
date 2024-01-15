# Lotus Education Challenge DE
 
## Este proyecto consistente en conectarse a una [RESTAPI](https://www.redhat.com/en/topics/api/what-is-a-rest-api), montar una base de datos local, después normalizarla y subirla a [AWS]( https://us-east-1.console.aws.amazon.com/rds/home?region=us-east-1). Usar herramientas como [Zeppelin]( https://zeppelin.apache.org/) para mostrar la normalización, así como Apache Spark para hacer consultas SQL.  

La arquitectura se desarrolló usando [Docker-compose](https://docs.docker.com/compose/) a través de tres servicios de contenedores. El primero es un contenedor con imagen de: Postgres, esto nos permite crear rápidamente una completa y operativa base de datos. El segundo servicio fue  con la imagen CetOs Linux donde bajamos la información de las apis, para poderlas ingestar a las bases de datos. Por último, se utilizo una segunda imagen CentOS que nos permitía pasar los datos normalizados a la base de datos de AWS. 

## Requerimientos: 
* [Docker](https://docs.docker.com/get-docker/)
* [Docker-Compose](https://docs.docker.com/compose/install/)
Nota: Al estar usando Docker, no es necesario instalar el resto de los módulos de Python, librerías ni dependencias individuales usando [pip](https://pypi.org/project/pip/)

---
## El programa se ejecuta de la siguiente forma:
Para iniciar todos los servicios, abrir una nueva terminal (CLI) y escribir los siguientes comandos:
```
docker-compose up -d --build     
```
Para tirar los servicios:
```
docker-compose down
```	
---
## Contenido adicional:
* [DBeaver](https://dbeaver.io/) - PostreSQL
* [Zeppelin notebook]( https://zeppelin.apache.org/) – Consultas
* [VS Code](https://code.visualstudio.com/) – IDE
* [AWS](https://us-east-1.console.aws.amazon.com/console) - Nube

## Desarrollado por: Adalberto Gonzalez
**celular**:  +52 2221639094
**Email**: adalgonlu@gmail.com
