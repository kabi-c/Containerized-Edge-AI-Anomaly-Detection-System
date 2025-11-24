// src/mqtt_publisher.hpp 
#pragma once 
#include <string> 
#include <iostream> 
#include <memory> 
20 
 
#include <mqtt/async_client.h> 
 
class MqttPublisher { 
    std::unique_ptr<mqtt::async_client> client; 
    std::string topic; 
public: 
    MqttPublisher(const std::string &broker_uri, const std::string &client_id, const 
std::string &topic_): 
        client(std::make_unique<mqtt::async_client>(broker_uri, client_id)), 
topic(topic_) {} 
 
    // TLS setup using files (optional). If empty, will use plain connection. 
    mqtt::connect_options create_tls_options(const std::string &ca_file, 
                                             const std::string &cert_file, 
                                             const std::string &key_file, 
                                             bool use_tls) { 
        mqtt::connect_options opts; 
        if(use_tls) { 
            mqtt::ssl_options sslopts; 
            sslopts.set_trust_store(ca_file); 
            sslopts.set_key_store(cert_file); 
            sslopts.set_private_key(key_file); 
            opts.set_ssl(sslopts); 
        } 
        return opts; 
    } 
 
    bool connect(const mqtt::connect_options &opts) { 
        try { 
            client->connect(opts)->wait(); 
            return true; 
        } catch(const mqtt::exception &e) { 
            std::cerr << "MQTT connect failed: " << e.what() << "\n"; 
            return false; 
21 
 
        } 
    } 
 
    void publish(const std::string &payload, int qos=1, bool retained=false) { 
        if(!client->is_connected()) { 
            std::cerr << "MQTT not connected; drop publish\n"; 
            return; 
        } 
        mqtt::message_ptr msg = mqtt::make_message(topic, payload); 
        msg->set_qos(qos); 
        msg->set_retained(retained); 
        client->publish(msg); 
    } 
 
    void disconnect() { 
        try { 
            if(client->is_connected()) 
                client->disconnect()->wait(); 
        } catch(...) {} 
    } 
 
    ~MqttPublisher() { disconnect(); } 
};
