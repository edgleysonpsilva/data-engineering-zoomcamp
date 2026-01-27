## Question 1. Understanding Docker images

To identify the pip version inside the image, I ran the container with an interactive bash entrypoint:

```bash
docker run -it --rm --entrypoint=bash python:3.13
```

Inside the container, I checked the version:

```bash
pip --version
```

Result:

```bash
root@daaeb283b1dc:/# pip --version
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
root@daaeb283b1dc:/# 
```
