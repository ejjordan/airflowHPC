
In the scalems scripts, make sure that the webserver is started on 8080:
```
airflow webserver --port 8080
```

On your local machine:
```
ssh -L 8080:localhost:8080 lumi
```
In that shell, run `hostname` to get the name of the login node


On the compute node (assuming airflow listens on 8080):
```
ssh -fN -R 8080:localhost:8080 uan02
```
Make sure you are using the correct login node name!


