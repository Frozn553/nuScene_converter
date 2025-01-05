import rclpy
from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

import rosbag2_py
import yaml
import os
import csv
 
from nav_msgs.msg import Odometry
from geometry_msgs.msg import PoseStamped, PoseWithCovarianceStamped
from sensor_msgs.msg import CompressedImage, CameraInfo, Imu
from tf2_msgs.msg import TFMessage
from autoware_auto_vehicle_msgs.msg import VelocityReport

def convert_rosbag2(config_path: str):

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    read_bag_path = config["read_bag_path"]
    write_bag_path = config["write_bag_path"]
    groundtruth_path = config["groundtruth_path"]

    csv_file = open(groundtruth_path, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["sec", "nsec", "x", "y"])

    camera_topics = []
    for cam in config.get("camera_list", []):
        cam_name = cam["camera_name"]
        camera_topics.append(f"/{cam_name}/image_rect_compressed")
        camera_topics.append(f"/{cam_name}/camera_info")

    if not os.path.exists(read_bag_path):
        print(f"[Error] Read bag not found: {read_bag_path}")
        return

    if os.path.exists(write_bag_path):
        print(f"[Warning] Output bag already exists, it will be overwritten: {write_bag_path}")
        os.remove(write_bag_path)
    reader = rosbag2_py.SequentialReader()
    storage_options = rosbag2_py.StorageOptions(
        uri=read_bag_path,
        storage_id='mcap'  
    )
    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format='cdr',
        output_serialization_format='cdr'
    )
    reader.open(storage_options, converter_options)

    topic_types = reader.get_all_topics_and_types()
    type_map = {topic_info.name: topic_info.type for topic_info in topic_types}

    writer = rosbag2_py.SequentialWriter()
    out_storage_options = rosbag2_py.StorageOptions(
        uri=write_bag_path,
        storage_id='sqlite3'
    )
    out_converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format='cdr',
        output_serialization_format='cdr'
    )
    writer.open(out_storage_options, out_converter_options)


    # 1) camera_list 
    for camera_name in camera_topics:
        if camera_name in type_map:
            writer.create_topic(
                rosbag2_py._storage.TopicMetadata(
                    name=camera_name,
                    type=type_map[camera_name],
                    serialization_format='cdr'
                )
            )

    # 2) /groundtruth_pose (PoseStamped)
    writer.create_topic(
        rosbag2_py._storage.TopicMetadata(
            name="/groundtruth_pose",
            type="geometry_msgs/msg/PoseStamped",
            serialization_format='cdr'
        )
    )
    # 3) /initialpose (PoseWithCovarianceStamped)
    writer.create_topic(
        rosbag2_py._storage.TopicMetadata(
            name="/initialpose",
            type="geometry_msgs/msg/PoseWithCovarianceStamped",
            serialization_format='cdr'
        )
    )
    # 4) /vehicle/status/velocity_status (VelocityReport)
    writer.create_topic(
        rosbag2_py._storage.TopicMetadata(
            name="/vehicle/status/velocity_status",
            type="autoware_auto_vehicle_msgs/msg/VelocityReport",
            serialization_format='cdr'
        )
    )
    # 5) /tf_static (TFMessage)
    tf_static_qos_yaml = """- history: 3\n  depth: 0\n  reliability: 1\n  durability: 1\n  deadline:\n    sec: 0\n    nsec: 0\n  lifespan:\n    sec: 0\n    nsec: 0\n  liveliness: 1\n  liveliness_lease_duration:\n    sec: 0\n    nsec: 0\n  avoid_ros_namespace_conventions: false """
    writer.create_topic(
        rosbag2_py._storage.TopicMetadata(
            name="/tf_static",
            type="tf2_msgs/msg/TFMessage",
            serialization_format='cdr',
            offered_qos_profiles=tf_static_qos_yaml
        )
    )
    # 6) /imu (sensor_msgs/msg/Imu)
    writer.create_topic(
        rosbag2_py._storage.TopicMetadata(
            name="/imu",
            type=type_map["/imu"],  
            serialization_format='cdr'
        )
    )

    first_odom_written = False
    first_tf_msg = None
    tf_timestamp= 0

    while reader.has_next():
        (topic, data, t) = reader.read_next()

        # (A) Camera topics
        if topic in camera_topics:
            writer.write(topic, data, t)
        
        # (B) /odom -> /groundtruth_pose + /initialpose (first entry) + /vehicle/status/velocity_status
        elif topic == "/odom":
            msg = deserialize_message(data,  get_message(type_map[topic]))
            # 1) groundtruth_pose (PoseStamped)
            pose_stamped = PoseStamped()
            pose_stamped.header = msg.header
            pose_stamped.pose = msg.pose.pose

            writer.write("/groundtruth_pose", serialize_message(pose_stamped), t)
            csv_writer.writerow([msg.header.stamp.sec, msg.header.stamp.nanosec, msg.pose.pose.position.x, msg.pose.pose.position.y])

            # 2) If it is the first entry of /odom, write /initialpose (PoseWithCovarianceStamped)
            if not first_odom_written:
                initial_pose = PoseWithCovarianceStamped()
                initial_pose.header.frame_id = "map"
                initial_pose.pose = msg.pose
                writer.write("/initialpose", serialize_message(initial_pose), t)
                first_odom_written = True
                tf_timestamp = t

            # 3) /vehicle/status/velocity_status (VelocityReport)
            vel_report = VelocityReport()
            vel_report.header = msg.header
            vel_report.longitudinal_velocity = msg.twist.twist.linear.x
            writer.write("/vehicle/status/velocity_status", serialize_message(vel_report), t)

        # (C) /tf -> Only take the first entry as /tf_static
        elif topic == "/tf":
            msg = deserialize_message(data, get_message(type_map[topic]))
            if first_tf_msg is None:
                first_tf_msg = msg

        # (D) /imu 
        elif topic == "/imu":
            writer.write(topic, data, t)

    # 5. After exiting the loop, write the first /tf to /tf_static
    if first_tf_msg and tf_timestamp:
        writer.write(
            "/tf_static",
            serialize_message(first_tf_msg),           
            tf_timestamp  # Use 0 as the timestamp
        )


    print("[Done] Converted bag written to:", write_bag_path)


def main():
    rclpy.init()
    config_path = config_path = os.path.dirname(os.path.realpath(__file__))+"/config/" + "config.yaml"
    convert_rosbag2(config_path)
    rclpy.shutdown()

if __name__ == "__main__":
    main()
