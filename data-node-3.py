#!/usr/bin/env python3
import os
import socket


NODE_ID = 3
SERVER_PORT = 6559
BUFFER_SIZE = 1024


def send_data_to_client(file_name, fsize):
    '''
    Choice 1
    '''
    try:
        with open(file_name, 'rb') as fs:
            data = fs.read(fsize)
    except:
        return 1
    return data


# Not required as the client is only sending the data to the replicas.
# def send_data_to_node(unique_id):
#     '''Choice 2'''
#     pass


def write_data_locally(obj_id, file_name, data):
    '''Choice 3'''
    dir_name = '/'.join(file_name.split('/')
                        [:-2]) + '/' + 'Node-' + str(NODE_ID) + '/' + obj_id + '/'
    file_name = dir_name + file_name.split('/')[-1]
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    print("Writing to: " + file_name)
    with open(file_name, 'wb') as fw:
        fw.write(data)


def listening_server():
    '''
    Server running as the data nodes which will trigger various
    functions and help in local reads and writes of the data chunks.
    '''
    ssFT = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssFT.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssFT.bind(('127.0.0.1', SERVER_PORT))
    ssFT.listen(5)

    while True:
        (conn, address) = ssFT.accept()
        print("Data node" + str(NODE_ID) + " got a connection")
        choice = conn.recv(BUFFER_SIZE)

        if int(choice.decode('utf-8')) == 3:
            '''This is for writing data locally.'''
            conn.send('ACK'.encode('utf-8'))
            obj_id = conn.recv(BUFFER_SIZE).decode('utf-8')
            print('Received obj_id is: ' + str(obj_id))
            conn.send('ACK'.encode('utf-8'))
            file_name = conn.recv(BUFFER_SIZE).decode('utf-8')
            print('Received file name is: ' + str(file_name))
            conn.send('ACK'.encode('utf-8'))
            print('Receiving the file size')
            msg = conn.recv(BUFFER_SIZE)
            fsize = int(msg.decode('utf-8'))
            print('File size is ' + str(fsize))
            print('Sending ACK...')
            conn.send('ACK'.encode('utf-8'))
            data = conn.recv(fsize * 2)
            print('Writing to the file')
            write_data_locally(obj_id, file_name, data)
            print("\n\n")
            conn.close()

        elif int(choice.decode('utf-8')) == 1:
            '''This is for reading data locally.'''
            conn.send('ACK'.encode('utf-8'))
            file_name = conn.recv(BUFFER_SIZE).decode('utf-8')
            print('File name is: ' + file_name)
            conn.send('ACK'.encode('utf-8'))
            try:
                fsize = os.path.getsize(file_name)
            except:
                conn.send(b"1")
                continue
            conn.send(str(fsize).encode('utf-8'))
            if conn.recv(3).decode('utf-8') == "ACK":
                data = send_data_to_client(file_name, fsize)
                conn.send(data)
            print("\n\n")
            conn.close()

        elif int(choice.decode('utf-8')) == 2:
            pass
        else:
            sys.exit("Invalid input!!")

    ssFT.close()


if __name__ == '__main__':
    listening_server()
