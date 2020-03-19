# Gregor
[![Python](https://img.shields.io/badge/python-3.5+-blue.svg?style=flat-square)](https://www.python.org)

Gregor is a program that produces fake data into an Apache Kafka topic for demo purposes

# Versions
## 0.0.2 - Known limitations
- It's only serializing into String. We must expand the possibilities to more type options.
- It's working with multiple partitions
- Partitions should be used with threads
- If the topics does not exist, we should created or give an error.
- Should release a CentOS Linux binaries
- Should be working on Sync or Async mode.

# How to use it?
First of all, you need to configure the Gregor.cfg file with all the options. 
See the sample one to get an idea. It's a simple INI format file. 

To use it just with python3

```shell script
# Check version
python3 Gregor.py -v

# Show data sample
python3 Gregor.py -d 
 
# Show the possible varibles of the fake data to generate
python3 Gregor.py -l 

# Executing indefenitely
python3 Gregor.py -s 
```

## License
See the [LICENSE](LICENSE.txt) file for license rights and limitations ([APACHE LICENSE 2.0](https://choosealicense.com/licenses/apache-2.0/#)).
