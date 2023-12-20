FROM ros:noetic-ros-base

# Update and install necessary packages
RUN apt-get update -y && \
    apt-get install -y python3-pip \
                       curl \
                       net-tools \
                       ros-noetic-cv-bridge

# python3 requirements
RUN pip3 install opencv-python==4.8.0.74 numpy==1.26.0 pytest==7.1
RUN pip3 install paho-mqtt hydra-core