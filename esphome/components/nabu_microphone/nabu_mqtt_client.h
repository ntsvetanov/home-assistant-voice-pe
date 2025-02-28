#pragma once

#include "mqttexample.h"
#include "mqttnet.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <vector>
#include <memory>


#ifndef WOLFMQTT_CLIENT_H
#error "mqtt_client_wolf.h not included"
#endif

#define TEST_TOPIC_COUNT 2
#define MAX_BUFFER_SIZE 1024
#define DEFAULT_CMD_TIMEOUT_MS 5000
#define DEFAULT_CON_TIMEOUT_MS 5000
#define DEFAULT_CLIENT_ID "default_client"
#define TEST_MESSAGE "Hello MQTT"
#define DEFAULT_KEEP_ALIVE_SEC 60
#define DEFAULT_QOS 1
#define WOLFMQTT_DEBUG_CLIENT

class MqttClientWrapper {
    private:
        MqttNet net;
        MqttClient client;
        byte *tx_buf;
        byte *rx_buf;
        MQTTCtx mqtt_ctx;
        
    public:
        MqttClientWrapper() : tx_buf(nullptr), rx_buf(nullptr) {}
    
        ~MqttClientWrapper() {
            cleanup();
        }
    
        bool initialize() {
            tx_buf = static_cast<byte*>(malloc(MAX_BUFFER_SIZE));
            rx_buf = static_cast<byte*>(malloc(MAX_BUFFER_SIZE));
            if (!tx_buf || !rx_buf) {
                std::cerr << "Memory allocation failed" << std::endl;
                free(tx_buf);
                free(rx_buf);
                return false;
            }
            int rc = MqttClientNet_Init(&net, &mqtt_ctx);
            std::cout << "MQTT Net Init: " << MqttClient_ReturnCodeToString(rc) << " (" << rc << ")\n";
            return rc == MQTT_CODE_SUCCESS;
        }
    
        bool setupClient() {
            int rc = MqttClient_Init(&client, &net, nullptr, tx_buf, MAX_BUFFER_SIZE, rx_buf, MAX_BUFFER_SIZE, DEFAULT_CMD_TIMEOUT_MS);
            std::cout << "MQTT Init: " << MqttClient_ReturnCodeToString(rc) << " (" << rc << ")\n";
            return rc == MQTT_CODE_SUCCESS;
        }
    
        bool connectToBroker(const std::string& broker, int port, int useTLS) {
            int rc = MqttClient_NetConnect(&client, broker.c_str(), port, DEFAULT_CON_TIMEOUT_MS, useTLS, nullptr);
            std::cout << "MQTT Socket Connect: " << MqttClient_ReturnCodeToString(rc) << " (" << rc << ")\n";
            return rc == MQTT_CODE_SUCCESS;
        }
    
        bool establishConnection(const std::string& clientId, const std::string& username, const std::string& password, int keep_alive_sec = 60, int clean_session = 1) {
            MqttConnect connect;
            memset(&connect, 0, sizeof(connect));
            connect.keep_alive_sec = keep_alive_sec;
            connect.clean_session = clean_session;
            connect.client_id = clientId.c_str();
            connect.username = username.c_str();
            connect.password = password.c_str();
            
            int rc = MqttClient_Connect(&client, &connect);
            std::cout << "MQTT Connect: " << MqttClient_ReturnCodeToString(rc) << " (" << rc << ")\n";
            return rc == MQTT_CODE_SUCCESS;
        }
    
        void publishMessage(const std::string& topic, const std::string& message, int qos) {
            MqttPublish publish;
            memset(&publish, 0, sizeof(publish));
            publish.retain = 0;
            publish.qos = static_cast<MqttQoS>(qos);
            publish.duplicate = 0;
            publish.topic_name = topic.c_str();
            publish.packet_id = mqtt_get_packetid();
            publish.buffer = (byte*)message.c_str();
            publish.total_len = static_cast<word16>(message.length());
            
            int rc = MqttClient_Publish(&client, &publish);
            std::cout << "MQTT Publish: Topic " << topic << ", Status: " << MqttClient_ReturnCodeToString(rc) << " (" << rc << ")\n";
        }
    
        void cleanup() {
            MqttClient_Disconnect(&client);
            MqttClient_NetDisconnect(&client);
            MqttClientNet_DeInit(&net);
            free(tx_buf);
            free(rx_buf);
            std::cout << "MQTT resources cleaned up.\n";
        }
    };