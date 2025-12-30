# 国网实时数仓测试环境 Dockerfile
FROM xuyangzzz/delta_join_example:1.0

# 设置环境变量
ENV HTTP_PROXY=http://host.docker.internal:7890
ENV HTTPS_PROXY=http://host.docker.internal:7890
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

# 设置工作目录
WORKDIR /app

# 配置代理
RUN echo 'Acquire::http::Proxy "http://host.docker.internal:7890";' > /etc/apt/apt.conf.d/proxy.conf && \
    echo 'Acquire::https::Proxy "http://host.docker.internal:7890";' >> /etc/apt/apt.conf.d/proxy.conf

# 添加PostgreSQL官方源
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list

# 一次性安装所有需要的包（包括sudo）
RUN apt-get update && apt-get install -y \
    openssh-server \
    postgresql-13 \
    postgresql-client-13 \
    postgresql-contrib-13 \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    wget \
    vim \
    supervisor \
    mysql-client \
    openjdk-11-jdk \
    net-tools \
    procps \
    software-properties-common \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# 配置SSH服务
RUN mkdir /var/run/sshd && \
    echo 'root:root123' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    ssh-keygen -A

# 生成SSH密钥对并配置免密登录
RUN ssh-keygen -t rsa -b 4096 -f /root/.ssh/id_rsa -N "" && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/authorized_keys && \
    chmod 700 /root/.ssh

# 配置Python pip代理
RUN mkdir -p /root/.pip && \
    echo '[global]' > /root/.pip/pip.conf && \
    echo 'proxy = http://host.docker.internal:7890' >> /root/.pip/pip.conf

# 安装Python依赖
RUN pip3 install --upgrade pip && \
    pip3 install psycopg2-binary pymysql pandas numpy faker requests psutil

# 创建应用目录结构
RUN mkdir -p /app/{scripts,sql,config,logs,data}

# 复制配置文件和脚本（这些文件变化频繁，放在后面）
COPY scripts/ /app/scripts/
COPY sql/ /app/sql/
COPY config/ /app/config/

# 配置PostgreSQL
RUN service postgresql start && \
    sudo -u postgres createdb power_grid && \
    sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';" && \
    sudo -u postgres psql -c "CREATE USER flink WITH PASSWORD 'flink' SUPERUSER;" && \
    service postgresql stop

# 配置PostgreSQL允许外部连接
RUN echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/13/main/pg_hba.conf && \
    echo "listen_addresses = '*'" >> /etc/postgresql/13/main/postgresql.conf && \
    echo "wal_level = logical" >> /etc/postgresql/13/main/postgresql.conf && \
    echo "max_replication_slots = 4" >> /etc/postgresql/13/main/postgresql.conf && \
    echo "max_wal_senders = 4" >> /etc/postgresql/13/main/postgresql.conf

# 下载并安装Apache Doris 2.1.5（稳定版）
RUN cd /opt && \
    wget -c --no-check-certificate \
    https://archive.apache.org/dist/doris/2.1.5/apache-doris-2.1.5-bin-x64.tar.gz || \
    wget -c --no-check-certificate \
    https://mirrors.tuna.tsinghua.edu.cn/apache/doris/2.1.5/apache-doris-2.1.5-bin-x64.tar.gz && \
    tar -xzf apache-doris-2.1.5-bin-x64.tar.gz && \
    mv apache-doris-2.1.5-bin-x64 doris && \
    rm apache-doris-2.1.5-bin-x64.tar.gz

# 安装Grafana
RUN wget -q -O - https://packages.grafana.com/gpg.key | apt-key add - && \
    echo "deb https://packages.grafana.com/oss/deb stable main" | tee -a /etc/apt/sources.list.d/grafana.list && \
    apt-get update && apt-get install -y grafana && \
    rm -rf /var/lib/apt/lists/*

# 配置Supervisor
COPY config/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# 设置权限
RUN chmod +x /app/scripts/*.sh && \
    chmod +x /app/scripts/*.py

# 暴露端口
EXPOSE 22 5432 8081 8084 9123 9030 8030 3000

# 启动命令
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]