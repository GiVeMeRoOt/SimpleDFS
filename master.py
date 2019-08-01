#!/usr/bin/env python3
import os
import sys
import ast
import math
import socket
from random import randrange


# Global Variables
NODES = 3
SERVER_PORT = 5558
BUFFER_SIZE = 1024


def get_absolute_path(rel_path):
    '''
    Required for converting a relative path to an absolute path.
    '''
    dirname = os.path.dirname(__file__)
    return os.path.join(dirname, rel_path)


def write_nodes_to_block_mapping(size, obj_id):
    '''
    The client program calls this to get a new mapping for chunks of
    data and the nodes they will be present on. It also persists this
    mapping by writing into the file(block_list).
    '''
    block_mapping = {}
    for i in range(int(math.ceil(size / 1024.0))):
        selected_node_id_1 = randrange(NODES) + 1
        selected_node_id_2 = randrange(NODES) + 1
        while selected_node_id_1 is selected_node_id_2:
            selected_node_id_2 = randrange(NODES - 1) + 1
        block_mapping[i] = [selected_node_id_1, selected_node_id_2]
    with open(get_absolute_path('blocks_list'), 'a') as blocks_list:
        print('Writing to the blocks_list file')
        blocks_list.write(str({obj_id: block_mapping}) + "\n")
    return {obj_id: block_mapping}


def get_nodes_to_block_mapping(obj_id):
    '''
    This function reads from the block_lists file and returns the
    mapping to the client function for retreiving of the chunks of
    data from the different nodes.
    '''
    with open(get_absolute_path('blocks_list'), 'r') as blocks_list:
        for i in blocks_list.readlines():
            try:
                found = ast.literal_eval(i.strip())[obj_id]
            except BaseException:
                continue
            else:
                return ast.literal_eval(i.strip())[obj_id]
        sys.exit("No blocks found as per the Object ID")


def listening_server():
    '''
    Primary function which keeps listening for new connections and
    calls the respective functions as requested by the client.
    '''
    ssFT = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssFT.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssFT.bind(('127.0.0.1', SERVER_PORT))
    ssFT.listen(5)
    while True:
        (conn, address) = ssFT.accept()
        print("Server got a connection")
        choice = conn.recv(BUFFER_SIZE)

        if int(choice.decode('utf-8')) == 1:
            '''This is for getting a new block mapping.'''
            conn.send('ACK'.encode('utf-8'))
            size = conn.recv(BUFFER_SIZE).decode('utf-8')
            print('Received size request is: ' + str(size))
            conn.send('ACK'.encode('utf-8'))
            obj_id = conn.recv(BUFFER_SIZE).decode('utf-8')
            print('Received object id: ' + str(obj_id))
            conn.send('ACK'.encode('utf-8'))
            block_mapping = write_nodes_to_block_mapping(
                int(size), str(obj_id))
            print('The block mapping is: ' + str(block_mapping))
            conn.send(str(block_mapping).encode('utf-8'))
            conn.close()

        elif int(choice.decode('utf-8')) == 2:
            '''This is for getting an existing block mapping.'''
            conn.send('ACK'.encode('utf-8'))
            obj_id = conn.recv(BUFFER_SIZE).decode('utf-8')
            print('Received object id: ' + str(obj_id))
            conn.send('ACK'.encode('utf-8'))
            block_mapping = get_nodes_to_block_mapping(str(obj_id))
            print('The block mapping is: ' + str(block_mapping))
            conn.send(str(block_mapping).encode('utf-8'))
            conn.close()

    ssFT.close()
    return 0


if __name__ == '__main__':
    listening_server()
