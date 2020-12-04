# CPTS415_G6
  
  This repo contains the code base for CPTS 415 Big Data Final Project
  
  Our Project is based off of creating an Airline Search Engine which has also been outfitted to showcase scalability of algorithms used, using mock datasets. The Project is comprised of two pieces: sequential algoriths and distributed algorithms.
  
  The Sequential Algorithm is written in Python 3.6. The Distributed Algorithm is coded in Scala and designed to work within a AWS linux environment running a Spark instance.
  
  # Sequential Algorithm
  Coded in Python 3.6 the user will need to install Python 3 on their device.
  * Libraries used:
  * NetworkX
  * Pandas  
  * Numpy 
  * tqdm
  ```
  pip install networkx pandas numpy tqdm
  ```
  
  To run the sequential program in linux, navigate to sequential folder and run 
  ```
  python3 graphing_Data.py
  ```
    
  # Distributed Algorithm
  The instance created on AWS is a r5a.2xlarge running ubuntu 18.04 with a 300GB Spinny Disk Drive (~.48/hr)
  
  The project utilizes Spark 3.0.1 to run on the system
  
  ## Setup Instance
  
  ### Install Java
  https://docs.datastax.com/en/jdk-install/doc/jdk-install/installOpenJdkDeb.html
  
  ### Install SBT
  See https://www.scala-sbt.org/release/docs/Installing-sbt-on-Linux.html#Ubuntu+and+other+Debian-based+distributions
```
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt
```
  
  ### Setup Spark
```
curl https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz -o spark-3.0.1-bin-hadoop2.7.tgz
tar -xzf spark-3.0.1-bin-hadoop2.7.tgz
```
  
  ### Starting Spark Cluster
```
./spark-3.0.1-bin-hadoop2.7/sbin/start-all.sh
```
  ## Running Distributed Code
  
  ### Check Cluster
  1. If running EC2 Instance you will need to get the public DNS and paste into web browser followed by ":8080"
    Ex) ec2-34-222-173-189.us-west-2.compute.amazonaws.com:8080
  2. If running local you can type in your preferred web browser localhost:8080
 
 ### Submitting to Cluster
  * Go into root folder of distributed algorithm and run ```sbt assembley```
  * Locate the jar file in path: ```target/scala-2.12/CPTS-415-Project-assembly-0.1.0.jar```
  * Submit to Spark
    * Use the directions above to view spark cluster on web browser to get the master url
    * Then run the following command ```./spark-3.0.1-bin-hadoop2.7/bin/spark-submit --master (Master URL) (Path to Jar)```
  * You can see the job is running by going to ```localhost:8080```
  * You can see active jobs in ```localhost:4040```
  
  ### Debugging on Local Device using IDE
  * Using Intellij Idea with Scala and sbt plug-in
  * Within the main code uncomment ```.master(local[*])``` to set the master node to your device
  * Run the code
  * Remember to comment out ```.master(local[*])``` when submitting to spark cluster or it will not run
