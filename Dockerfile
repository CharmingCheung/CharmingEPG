#从官方Python基础镜像开始。
FROM python:3.15.0-slim-trixie

# 应用 Debian 已发布的安全修复；发布时配合 --pull --no-cache 避免旧层缓存。
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

#将当前工作目录设置为/code。
#这是我们放置requirements.txt文件和app目录的位置。
WORKDIR /code

#将符合要求的文件复制到/code目录中。
#首先仅复制requirements.txt文件，而不复制其余代码。
#由于此文件不经常更改，Docker 将检测到它并在这一步中使用缓存，从而为下一步启用缓存。
COPY ./requirements.txt /code/requirements.txt

#安装需求文件中的包依赖项。
#--no-cache-dir 选项告诉 pip 不要在本地保存下载的包，因为只有当 pip 再次运行以安装相同的包时才会这样，但在与容器一起工作时情况并非如此。
#
#笔记
#--no-cache-dir 仅与 pip 相关，与 Docker 或容器无关。
#--upgrade 选项告诉 pip 升级软件包（如果已经安装）。
#
#因为上一步复制文件可以被 Docker 缓存 检测到，所以此步骤也将 使用 Docker 缓存（如果可用）。
#
#在开发过程中一次又一次构建镜像时，在此步骤中使用缓存将为你节省大量时间，而不是每次都下载和安装所有依赖项。
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir --upgrade -r /code/requirements.txt \
    && python -m pip check \
    # pip 内置的旧 urllib3/msgpack 也会带入漏洞，运行服务无需保留安装器。
    && python -m pip uninstall --yes pip

#将“./app”目录复制到“/code”目录中。
#
#由于其中包含更改最频繁的所有代码，因此 Docker 缓存不会轻易用于此操作或任何后续步骤。
#
#因此，将其放在Dockerfile接近最后的位置非常重要，以优化容器镜像的构建时间。
COPY ./app /code/app

#设置命令来运行 uvicorn 服务器。
#
#CMD 接受一个字符串列表，每个字符串都是你在命令行中输入的内容，并用空格分隔。
#
#该命令将从 当前工作目录 运行，即你上面使用WORKDIR /code设置的同一/code目录。
#
#因为程序将从/code启动，并且其中包含你的代码的目录./app，所以Uvicorn将能够从app.main中查看并importapp。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
