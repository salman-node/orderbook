module.exports = {
    apps: [
      { 
        name: "zookeeper",
        script: "./zookeeper-server-start.bat",
        args: "./config/zookeeper.properties",
        cwd: "C:/kafka_2.13-3.9.0/bin/windows",
        autorestart: true,
      },
      { 
        name: "kafka",
        script: "./kafka-server-start.bat",
        args: "./config/server.properties",
        cwd: "C:kafka_2.13-3.9.0/bin/windows",
        autorestart: true,
      }
    ]
  };
  