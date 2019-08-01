#!/usr/bin/env python3

# Import statements
import os
import sys
import ast
import socket
import string
import random


# Global Variables
CHUNK_SIZE = 1024
NODE1_PORT = 6557
NODE2_PORT = 6558
NODE3_PORT = 6559
SERVER_PORT = 5558
BUFFER_SIZE = 1024


def get_absolute_path(rel_path):
    '''
    Required for converting a relative path to an absolute path.
    '''
    dirname = os.path.dirname(__file__)
    return os.path.join(dirname, rel_path)


def get_new_nodes_to_block_mapping(size, obj_id):
    '''
    Send option 1 to the master server to request for a new
    block mapping and return it to the caller of this function.
    '''
    csFT = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        csFT.connect(('127.0.0.1', SERVER_PORT))
    except BaseException:
        sys.exit("Master is unreachable")
    csFT.send(str(1).encode('utf-8'))

    if csFT.recv(3).decode('utf-8') == "ACK":
        csFT.send(str(size).encode('utf-8'))
    else:
        sys.exit('Server did not ACK our request.')
    if csFT.recv(3).decode('utf-8') == "ACK":
        csFT.send(str(obj_id).encode('utf-8'))
    else:
        sys.exit('Server did not accept our size request.')
    if csFT.recv(3).decode('utf-8') == "ACK":
        block_mapping = csFT.recv(BUFFER_SIZE).decode('utf-8')
    csFT.close()
    return block_mapping


def get_nodes_to_block_mapping(obj_id):
    '''
    Send option 2 to the master server to request for an existing
    block mapping.
    '''
    csFT = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        csFT.connect(('127.0.0.1', SERVER_PORT))
    except BaseException:
        sys.exit("Master is unreachable")
    csFT.send(str(2).encode('utf-8'))

    if csFT.recv(3).decode('utf-8') == "ACK":
        csFT.send(str(obj_id).encode('utf-8'))
    else:
        sys.exit('Server did not accept our request.')
    if csFT.recv(3).decode('utf-8') == "ACK":
        block_mapping = csFT.recv(BUFFER_SIZE).decode('utf-8')
    csFT.close()
    return block_mapping


def display_file_mapping():
    '''
    Read the file mapping from obj_id_to_filename and display
    it to the user and ask for the choice of the user.
    '''
    with open(get_absolute_path('obj_id_to_filename'), 'r') as file_list:
        obj_id_list = []
        file_name_list = []
        for i in file_list.readlines():
            mapping_dict = ast.literal_eval(i.strip())
            obj_id_list += mapping_dict.keys()
            file_name_list += mapping_dict.values()
        choice_counter = 0
        for i in file_name_list:
            print(str(choice_counter) + ". " + i)
            choice_counter += 1
        choice = input("Choose the file you want to retrieve: ")
        if int(choice) > len(file_name_list) - 1:
            sys.exit('Invalid choice!!')
        else:
            return (obj_id_list[int(choice)], file_name_list[int(choice)])


def send_data(obj_id, file_name, node_id):
    '''
    Send option 3 to the data node for local write on the individual
    data node. The node is decided based on the node_mapping.
    '''
    node_mapping = {1: NODE1_PORT, 2: NODE2_PORT, 3: NODE3_PORT}
    csFT = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        csFT.connect(('127.0.0.1', node_mapping[node_id]))
    except socket.error as e:
        return 1
    csFT.send(str(3).encode('utf-8'))

    if csFT.recv(3).decode('utf-8') == "ACK":
        csFT.send(str(obj_id).encode('utf-8'))
    else:
        sys.exit('Server did not ACK our request.')
    if csFT.recv(3).decode('utf-8') == "ACK":
        csFT.send(str(file_name).encode('utf-8'))
    else:
        sys.exit('Server did not accept our size request.')
    if csFT.recv(3).decode('utf-8') == "ACK":
        fsize = os.path.getsize(file_name)
        #print("File size is: "+str(fsize))
        csFT.send(str(fsize).encode('utf-8'))
    else:
        sys.exit('Server did not accept our file size.')
    if csFT.recv(3).decode('utf-8') == "ACK":
        with open(file_name, 'rb') as fs:
            data = fs.read(fsize)
            csFT.send(data)
    csFT.close()
    return 0


def get_data(file_name, node_id):
    '''
    Send option 1 to the data node for local read from the
    individual data node.
    '''
    node_mapping = {1: NODE1_PORT, 2: NODE2_PORT, 3: NODE3_PORT}
    csFT = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        csFT.connect(('127.0.0.1', node_mapping[node_id]))
    except socket.error as e:
        return 1
    csFT.send(str(1).encode('utf-8'))
    if csFT.recv(3).decode('utf-8') == "ACK":
        csFT.send(str(file_name).encode('utf-8'))
    else:
        sys.exit('Server did not accept our request.')
    if csFT.recv(3).decode('utf-8') == "ACK":
        fsize = csFT.recv(BUFFER_SIZE)
        csFT.send('ACK'.encode('utf-8'))
        if fsize == b'1':
            return 1
    else:
        sys.exit('Server did not accept our filename.')
    return csFT.recv(int(fsize))


# Not required since the client is directly sending to both the nodes.
# def forward_data(unique_id, node_id, forward_node_id):
#     pass


def break_to_blocks(fromfile, todir, obj_id, chunksize=CHUNK_SIZE):
    '''
    Break the data to smaller chunks, each of CHUNK_SIZE and name
    chunks with a part number which will be used to join back the
    chunks to form the original data.
    '''
    if not os.path.exists(todir):                  # caller handles errors
        os.mkdir(todir)                            # make dir, read/write parts
    else:
        for fname in os.listdir(todir):            # delete any existing files
            os.remove(os.path.join(todir, fname))
    partnum = 0
    input = open(fromfile, 'rb')                   # use binary mode on Windows
    while True:                                    # eof=empty string from read
        chunk = input.read(chunksize)              # get next part <= chunksize
        if not chunk:
            break
        partnum = partnum + 1
        filename = os.path.join(todir, (str(obj_id) + '%04d' % partnum))
        fileobj = open(filename, 'wb')
        fileobj.write(chunk)
        fileobj.close()
    input.close()
    assert partnum <= 9999                         # join sort fails if 5 digits
    return 0


# Not required since we are concatenating the files as we receive them.
# def join_from_blocks(fromdir, tofile, obj_id, readsize = CHUNK_SIZE):
#     with open(tofile, 'wb') as output:
#         parts  = os.listdir(fromdir)
#         parts.sort(  )
#         for filename in parts:
#             filepath = os.path.join(fromdir, filename)
#             with open(filepath, 'rb') as fileobj:
#                 while 1:
#                     filebytes = fileobj.read(readsize)
#                     if not filebytes: break
#                     output.write(filebytes)


def interactive_client():
    '''
    An infinite loop which will accept responses and be used
    to trigger client operations.
    '''
    print('Welcome to the Distributed File System')
    while True:
        print('1. Retrieve existing data')
        print('2. Store new data')
        print('3. Exit')
        user_choice = input('Choose any of the above: ')

        if user_choice == '1':
            (read_obj_id, write_file_name) = display_file_mapping()
            print("You chose to retrieve: " + read_obj_id)
            block_mapping = get_nodes_to_block_mapping(read_obj_id)
            print("Block mapping: " + block_mapping)
            blocks_dict = ast.literal_eval(block_mapping.strip())
            data = b""
            parts_counter = 1
            for parts in blocks_dict:
                file_name = get_absolute_path("Node-" + str(blocks_dict[parts][0]) + "/" + read_obj_id + "/")
                file_name = os.path.join(
                    file_name, read_obj_id + '%04d' % parts_counter)
                temp_data = get_data(file_name, blocks_dict[parts][0])
                if temp_data == 1:
                    temp_data = get_data(file_name, blocks_dict[parts][1])
                    if temp_data == 1:
                        sys.exit(
                            "Not enough data nodes are up to retrieve the files")
                data += temp_data
                parts_counter += 1

            out_file = write_file_name + ".backup"
            with open(out_file, "wb") as output_file:
                output_file.write(data)
            print("Finished writing to the file.")
            print("\n")

        elif user_choice == '2':
            user_file_name = input(
                'Enter the file path that you want to store: ')
            fsize = os.path.getsize(user_file_name)
            todir = get_absolute_path("test-split/")
            print("Splitting the files into smaller chunks")
            obj_id = ''.join(random.choice(string.ascii_letters)
                             for i in range(15))
            print('Object ID is: ' + obj_id)
            break_to_blocks(user_file_name, todir, obj_id, CHUNK_SIZE)
            block_mapping = get_new_nodes_to_block_mapping(fsize, obj_id)
            blocks_dict = ast.literal_eval(block_mapping.strip())[obj_id]
            print('Block mapping: ' + str(blocks_dict))

            for nodes_data in blocks_dict:
                nodes_data += 1
                file_to_send = os.path.join(
                    todir, obj_id + '%04d' % nodes_data)
                #print("Sending file: "+file_to_send+" to Node-"+str(blocks_dict[nodes_data-1][0]))
                send_status_1 = send_data(
                    obj_id, file_to_send, blocks_dict[nodes_data - 1][0])
                send_status_2 = send_data(
                    obj_id, file_to_send, blocks_dict[nodes_data - 1][1])
                if send_status_1 == 1 and send_status_2 == 1:
                    sys.exit("One block was not written to any of the nodes!")

            with open(get_absolute_path('obj_id_to_filename'), 'a') as id_filename:
                print('Writing to the obj_id_to_filename file')
                id_filename.write(str({obj_id: user_file_name}) + "\n")

            print('All files sent to the respective nodes')
            #print(str(nodes_data) + ":" + str(blocks_dict[nodes_data][0]))
            print('\n')

        elif user_choice == '3':
            sys.exit('Exiting the program')
        else:
            print('Invalid choice!!')
            continue


if __name__ == '__main__':
    interactive_client()
