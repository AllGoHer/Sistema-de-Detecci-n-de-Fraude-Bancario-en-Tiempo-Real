# 🚨 Sistema-de-Detecci-n-de-Fraude-Bancario-en-Tiempo-Real
________________________________________________________________________________________________________________________________________________________________________________________________________________
## ✦ Arquitectura de Autoridad Propia.✦

***Un pipeline de Streaming con Apache Flink construido <mark>SIN JAVA</mark>. Potencia de Flink combinada con la simplicidad de <mark>Python</mark> y <mark>SQL</mark>.***
________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/635450ed-4a22-4267-b4aa-90dca6747fe7)
________________________________________________________________________________________________________________________________________________________________________________________________________________
### 📌 Nota de Arquitectura: El enfoque "Zero Java" en Apache Flink

Este proyecto fue diseñado, desarrollado y desplegado al 100% desde cero por mí, con una premisa de arquitectura clave: **Cero código Java**.

En la industria, Apache Flink es temido por su curva de aprendizaje base en Java/Scala (API DataStream). Este proyecto **rompe ese paradigma**, demostrando que se puede organizar un pipeline de Streaming de producción (1000 TPS, Event Time, Watermarks y Ventanas Temporales) utilizando exclusivamente Python para la ingesta/stateful generation y Flink SQL para el proceso completo.

El resultado es una arquitectura de datos moderna, altamente legible, mantenible y accesible para cualquier equipo de Data Engineering basado en Python/SQL, <mark>sin sacrificar el poder de cálculo de Flink</mark>. Demostrando así, que no necesitas un equipo de desarrolladores Java para mantener un sistema de detección de fraude en tiempo real de nivel empresarial; <mark>la lógica de negocio vive donde debe vivir: en SQL</mark>.

________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://img.shields.io/badge/License-MIT-yellow.svg)
![image](https://img.shields.io/badge/Apache%2520Flink-1.17.1-blue)
![image](https://img.shields.io/badge/Apache%2520Kafka-3.9.0-black)
![image](https://img.shields.io/badge/MySQL-8.0-orange)
![image](https://img.shields.io/badge/Grafana-10.x-green)
![image](https://img.shields.io/badge/Docker-24.x-blue)
![image](https://github.com/user-attachments/assets/ef9e51f1-7111-4af0-bb7f-e8fcb270e86a)


**🎯 El reto: el fraude bancario en tiempo real**

*Cada segundo se producen miles de transacciones fraudulentas en todo el mundo. El procesamiento por lotes tradicional detecta el fraude horas más tarde, cuando el dinero ya se ha perdido*.

Este proyecto resuelve ese problema. Se trata de un canal de streaming de alta velocidad que detecta transacciones bancarias fraudulentas en tiempo real, utilizando Apache Flink como motor de procesamiento y Kafka como intermediario de eventos.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura: canalización de streaming de extremo a extremo

![iamge](https://github.com/user-attachments/assets/b7205bcd-2f0f-4b99-b624-d8b2cb27a0f5)
![image](https://github.com/user-attachments/assets/43dc804e-ebed-45c9-96b5-1c3f220440ba)

________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🔧 Análisis en profundidad de la arquitectura
________________________________________________________________________________________________________________________________________________________________________________________________________________
**1. Generación de datos (productor)**

* Simula más de 1000 transacciones por segundo

* Incorpora patrones de fraude:

  * Picos de velocidad: > 100 transacciones en 10 segundos por parte de un mismo usuario

  * Comportamiento de deriva: usuarios que cambian repentinamente de patrones

  * Indicadores directos de fraude: para probar la detección

* Utiliza 100 usuarios sintéticos con perfiles realistas

**2. Procesamiento de flujos (Flink)**

* Ventanas «tumbling»: ventanas de 10 segundos sin solapamiento

* Procesamiento con estado: realiza un seguimiento del comportamiento del usuario a lo largo del tiempo

* Reglas de detección de fraude:

  * Cualquier transacción con <mark>is_fraud=1</mark> → ALERTA

  * Cualquier usuario con > 100 transacciones/10 s → ALERTA

* **Semántica Exactly-Once**: garantiza la coherencia de los datos

**3. Almacenamiento y visualización**

* MySQL: almacena métricas agregadas y alertas

* Grafana: paneles en tiempo real con:

  * Alertas de detección de fraude en directo

  * Supervisión de la velocidad de las transacciones

  * Análisis del comportamiento de los usuarios

  * Tendencias históricas de fraude
________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/e9b79576-6494-4eff-9a9a-596cc0a39220)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📊 Indicadores clave de Desempeño (KPI)

| KPI | Descripción | Representación gráfica |
|-----|-------------|------------------------|
| 🚨 Alertas de fraude | Total de transacciones sospechosas detectadas | Big Number |
| ⚡Velocidad de transacciones | Número de transacciones por ventana de 10 segundos por usuario | Gauge Chart |
| 👤 Principales autores de fraude | Usuarios con más alertas de fraude | Bar Chart |
| 💰 Importe medio del fraude | Importe medio de las transacciones marcadas | Statistics |
| 📈 Tendencia del fraude | Detección de fraudes a lo largo del tiempo | Line Chart |

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🛠️ Pila tecnológica

| Capa | Tecnología | Finalidad |
|------|------------|-----------|
| Fuente de datos | Python + Kafka Producer | Simular transacciones en tiempo real |
| Broker de mensajes | Apache Kafka (KRaft) | Transmisión de eventos de alto rendimiento |
| Procesamiento | Apache Flink 1.17.1 | Procesamiento de flujos y detección de fraudes |
| Almacenamiento | MySQL 8.0 | Persistencia de métricas y alertas |
| Visualización | Grafana 10.x | Cuadros de mando en tiempo real |
| Orquestación | Docker Compose | Infraestructura en contenedores |

____________________________________________________________________________________________________________________________________
* **Ingesta (Productor):** Python 3.11 con . Utiliza múltiples hilos (multiproceso) y para transacciones similares legítimas y ataques de velocidad (Velocity Bursts).kafka-python-ngdataclasses.
* **Broker de Mensajes:** Apache Kafka 3.9.0 en modo KRaft (sin dependencias de Zookeeper).
* **Motor de Procesamiento:** Apache Flink 1.17.1. Se utiliza la API de Flink SQL para definir Marcas de agua, Hora del evento y ventanas Tumble de 10 segundos sin escribir código Java/Scala.
* **Alojamiento (Sink):** MySQL 8.0. (Decisión técnica: Se optó por MySQL sobre PostgreSQL/ClickHouse porque el conector JDBC nativo de Flink lo permite fuera de la caja en Windows, evitando errores de dialectos o dependencias faltantes).
* **Visualización:** Grafana configurada con actualización automática cada 5 segundos sobre consultas SQL directas a MySQL.
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/a3d7b514-4cd8-4637-956e-f8c405786306)
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📚Estructura del Proyecto

![image](https://github.com/user-attachments/assets/cbd74f26-4484-4a40-936b-c984183e2211)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## Guía de Implementación Paso a Paso
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 1: Configuración de la Infraestructura (Docker)**

Crear el archivo .

Notas críticas de la configuración:
[docker-compose.yml]()

1. Se usa para evitar el error típico de MySQL 8.mysql_native_passwordPublic Key Retrieval is not allowed.
   
2. Se exponen variables de entorno natural de Flink () en lugar de para evitar problemas de análisis en Windows CMD.FLINK_TASK_MANAGER_MEMORYFLINK_PROPERTIES.
   
3. El puerto de MySQL se mapea al para no chocar con instalaciones locales.3307

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
