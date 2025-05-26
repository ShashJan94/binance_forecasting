@echo off
SET KAFKA_DIR=C:\kafka

REM ====== Start Zookeeper ======
echo Starting Zookeeper...
start "Zookeeper" %KAFKA_DIR%\bin\windows\zookeeper-server-start.bat %KAFKA_DIR%\config\zookeeper.properties

REM ====== Wait for Zookeeper to be ready ======
:zk_wait
echo Checking if Zookeeper is up on port 2181...
powershell -Command "try { $socket = New-Object Net.Sockets.TcpClient('localhost',2181); if ($socket.Connected) { exit 0 } else { exit 1 } } catch { exit 1 }"

echo Zookeeper is running!

REM ====== Start Kafka Broker ======
echo Starting Kafka Broker...
start "Kafka Broker" %KAFKA_DIR%\bin\windows\kafka-server-start.bat %KAFKA_DIR%\config\server.properties

REM ====== Wait for Kafka to initialize (optional, similar logic as above for port 9092) ======
REM ... (can add check for port 9092 if needed) ...

REM ====== Ask user if they want to create a topic ======
set /p create_topic=Do you want to create a Kafka topic? (y/n):
if /i not "%create_topic%"=="y" goto end

set /p topic_name=Enter topic name (default: raw-ticks):
if "%topic_name%"=="" set topic_name=raw-ticks

echo Checking if topic "%topic_name%" exists...
%KAFKA_DIR%\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 | findstr /i /c:"%topic_name%"
if %errorlevel%==0 (
    echo Topic "%topic_name%" already exists. Skipping creation.
) else (
    echo Creating topic "%topic_name%"...
    %KAFKA_DIR%\bin\windows\kafka-topics.bat --create --topic %topic_name% --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
)

:end
echo.
echo ====== Zookeeper and Kafka should now be running. ======
pause
