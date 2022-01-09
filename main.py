import mmap
import datetime

DISKSIZE = 128*(2**20)
BLOCKSIZE = 4*(2**10)
BLOCKNUMBER = int(DISKSIZE/BLOCKSIZE)

# itera sobre os bits de um byte dizendo se é 0 ou não
def bits(int):
    mask = 0b10000000
    for i in range(8):
        if int & mask:
            yield (False, i)
        else:
            yield (True, i)
        mask = mask >> 1

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class iNode:
    """
    iNode
    nome -> 128B
    tipo -> 2B (uint 16 - 0:dir, 1:file)
    dono -> 30B (nome do dono)
    criado -> 4B (unsigned int timestamp)
    modificado -> 4B (unsigned int timestamp)
    168 bytes até aqui
    
    ponteiros -> 2B (uint 16 apontando para blocos)
    (4096-168)/2 = 1962 max blocos referenciados
    isso limita cada diretório a ter 1962 elementos
    e o tamanho máximo de arquivo a (1962*4096) = 8036352 bytes
    """

    def __init__(self, name, itype, created, modified, owner, table = []):
        self.name = name
        self.type = itype
        self.created = int(created)
        self.modified = int(modified)
        self.owner = owner
        self.table = table              # nao pode conter zeros

    def __repr__(self) -> str:
        return f"({self.name}, {self.owner}, {self.created}, {self.modified}, {self.table})"
    
    def toBytes(self):
        serialized = bytearray(BLOCKSIZE)
        index = 0
        # nome do arquivo/diretorio
        name = bytearray(self.name, encoding='utf-8')
        if len(name) > 128:
            raise Exception(f"Erro: o nome \"{self.name}\" possui um tamanho maior do que o máximo permitido")
        serialized[index: index+len(name)] = name
        index += 128
        
        # tipo
        serialized[index:index+2] = int.to_bytes(self.type, 2, 'big', signed=False)
        index += 2
        # criado
        serialized[index:index+4] = int.to_bytes(self.created, 4, 'big', signed=False)
        index += 4
        # modificado
        serialized[index:index+4] = int.to_bytes(self.modified, 4, 'big', signed=False)
        index += 4
        
        # dono
        ow = bytearray(self.owner, 'utf-8')
        if len(ow) > 30:
            raise Exception(f"Erro: o nome do dono \"{self.owner}\" possui um tamanho maior do que o máximo permitido")
        serialized[index:index+len(ow)] = ow
        index += 30

        # tabela de blocos
        if len(self.table) > 1962:
            raise Exception(f"Erro: tamanho máximo de referências excedido")

        null = int.to_bytes(65535, 2, 'big', signed=False)

        for block in self.table:
            serialized[index: index+2] = int.to_bytes(block, 2, 'big', signed=False)
            index += 2
            

        for i in range(len(self.table), 1962+2):
            serialized[index: index+2] = null
            index += 2
        
        return serialized
    
    @staticmethod
    def fromBytes(byteblock):
        blocks = [int.from_bytes(byteblock[i:i+2], 'big', signed=False) for i in range(168, 4096, 2)]
        return iNode(
            byteblock[0:128].decode('utf-8').rstrip('\00'),
            int.from_bytes(byteblock[128:130], 'big', signed=False),
            int.from_bytes(byteblock[130:134], 'big', signed=False),
            int.from_bytes(byteblock[134:138], 'big', signed=False),
            byteblock[138:168].decode('utf-8').rstrip('\00'),
            [block for block in blocks if block != 65535]
        )

class DiskManager:

    INODESTART = 2 * BLOCKSIZE
    DATASTART = 2758 * BLOCKSIZE

    """
    gerenciamento de blocos alocados:
        o disco possui 128MB e blocos de 4KB, para um total de 32768 blocos
        o status de cada bloco (livre ou o ocupado) será identificado por um bit
        então são necessários 4096 bytes (2 blocos) [0:2] em disco
    espaço para iNodes:
        o disco possuirá 2766 iNode's [2:2768] em disco
        o primeiro iNode sempre será a pasta raiz (bloco índice 2)
    espaço para dados de arquivos:
        será o restante (30000 blocos) [2768:32768] em disco
    """

    def __init__(self, diskpath, user='system', wipeDisk = False) -> None:

        if wipeDisk:
            bytearr = bytearray(DISKSIZE)
            bytearr[0:1] = int.to_bytes(224, 1, 'big', signed=False)
            bytearr[self.INODESTART: self.INODESTART + BLOCKSIZE] = (
                iNode(
                    'root', 0,
                    datetime.datetime.now().timestamp(), datetime.datetime.now().timestamp(),
                    'system'
                ).toBytes()
            )

            with open(diskpath, 'wb') as disk:
                disk.write(bytearr)
            

        d = open(diskpath, 'r+b')
        self.disk = mmap.mmap(d.fileno(), 0)
        self.user = user
        self.root = 2
        self.current_dir = [2]

    def _readBytes(self, start, end=None):
        # lê do disco os bytes no intervalo "start":"end"
        # se end nao for passado, lê apenas 1 byte
        if end is None:
            return self.disk[start:start+1]
        else:
            return self.disk[start:end]
    
    def _writeBytes(self, atIndex, bytes):
        # escreve "bytes" no disco a partir do byte "atIndex"
        self.disk[atIndex: atIndex+len(bytes)] = bytes
        self.disk.flush()
    
    @staticmethod
    def _blockify(bytes):
        # transforma um bytearray de tamanho n "bytes" em uma lista de bytearray's de tamanho BLOCKSIZE
        # se a divisão não for exata, faz padding
        blocks = []
        for i in range(0, len(bytes), BLOCKSIZE):
            b = bytes[i:i+BLOCKSIZE]
            blocks.append(b)

        if len(blocks) and len(blocks[-1]) != BLOCKSIZE:
            blocks[-1].extend(bytearray(BLOCKSIZE - len(blocks[-1])))

        return blocks
    
    def _allocate(self, type='inode') -> int:
        # aloca um bloco na tabela de alocação e retorna o índice
        if type == 'inode':
            byterange = self._readBytes(0, 221)
        elif type == 'data':
            byterange = self._readBytes(221, 2*BLOCKSIZE)
        else:
            raise Exception()
        
        # encontra o byte com algum bit 0
        byte_index = 0
        byte_value = 0
        while byte_index < len(byterange):
            byte_value = int.from_bytes(byterange[byte_index:byte_index+1], 'big', signed=False)
            if byte_value != 255:
                break
            else:
                byte_index += 1
        else:
            raise Exception('AllocationError')
        
        for (b, p) in bits(byte_value):
            if b:
                byte_value |= (128 >> p)
                block_index = 8*byte_index + p
                break
        
        self._writeBytes(byte_index, int.to_bytes(byte_value, 1, 'big', signed=False))

        return block_index
    
    def _deallocate(self, blockindex):
        # marca bloco como desalocado na tabela de alocação
        byte = blockindex // 8
        bit = blockindex % 8
        old = int.from_bytes(self._readBytes(byte), 'big', signed=False)
        new = ( old & ~(128 >> bit) )
        self._writeBytes(byte, int.to_bytes(new, 1, 'big', signed=False))
    
    def get_inode(self, idx):
        # carrega um inode de um bloco
        if idx < 2 or idx > 2768:
            raise Exception('Inode index out of range')

        blocks = idx * BLOCKSIZE
        return iNode.fromBytes(self._readBytes(blocks, blocks + BLOCKSIZE))
    
    def set_inode(self, idx, inode):
        # escreve um inode em disco
        self._writeBytes(idx*BLOCKSIZE, inode.toBytes())

    def _get_subdir(self, tbl, name):
        l, r = 0, len(tbl) - 1

        while l <= r:
            m = (l+r)//2

            temp_inode = self.get_inode(tbl[m])
            if temp_inode.name == name:
                return (True, m)
            if temp_inode.name > name:
                r = m - 1
            else:
                l = m + 1
            
        return (False, l)

    def mkdir(self, path, table=[]):
        # cria um novo diretório em um inode

        # se nao tiver / é na pasta atual
        if "/" in path:
            where_name = path.split("/")
            name = where_name[-1]
            where = self._resolvePath( "/".join(where_name[:-1]) )[0]
        else:
            where = self.current_dir[-1]
            name = path
        
        destiny = self.get_inode(where)
        
        if len(destiny.table) == 1962:
            raise Exception('Folder is full, it doesn\'t support more iNodes.')

        if destiny.type != 0:
            raise Exception(f'{destiny.name} is not a directory')
        
        # checa se já existe inode com mesmo nome
        (has, pos) = self._get_subdir(destiny.table, name)

        if has:
            raise FileExistsError(f'Directory "{name}" already exists')
        
        new_dir_block = self._allocate() # aloca um novo inode


        new_dir = iNode(name, 0, datetime.datetime.now().timestamp(), datetime.datetime.now().timestamp(), self.user, table)
        # print(f"creating {new_dir} at block {new_dir_block}")

        destiny.table.insert(pos, new_dir_block)
        self.set_inode(where, destiny)
        self.set_inode(new_dir_block, new_dir)

    def rmdir(self, where, name):

        parent = self.get_inode(where)

        (has, pos) = self._get_subdir(parent.table, name)
        
        if not has:
            raise FileNotFoundError(f'Directory "{name}" does not exist')

        dir_idx = parent.table[pos]
        dir_inode = self.get_inode(dir_idx)

        if len(dir_inode.table) > 0:
            raise Exception(f'Directory "{name}" is not empty')
        
        self._deallocate(dir_idx)
        parent.table.pop(pos)
        self.set_inode(where, parent)

    def _resolvePath(self, pathString):
        curr_path = self.current_dir.copy()
        tokens = pathString.split("/")

        if len(tokens) and tokens[0] == '':
            curr_path = curr_path[:1]
            tokens = tokens[1:]

        pos = 0
        for t in tokens:
            curr_node = curr_path[-1]
            
            curr_node = self.get_inode(curr_node)

            if t == '..':
                if len(curr_path) > 1:
                    curr_path.pop()
                continue
            elif t == '.' or t =='':
                continue

            if curr_node.type != 0:
                raise FileNotFoundError(f'{curr_node.name} is not a directory')

            (has, pos) = self._get_subdir(curr_node.table, t)

            if not has:
                raise FileNotFoundError(f"{curr_node.name}/{t} doens\'t exist")
            else:
                curr_path.append(curr_node.table[pos])
                
        # if len(curr_path) > 0:
        return (curr_path[-1], curr_path)
        # else:
        #     return (self.root, curr_path)
    
    def ls(self, where):
        # lista os diretórios/arquivos do dir atual
        node = self.get_inode(where)

        if node.type != 0:
            raise FileNotFoundError(f'{node.name} is not a directory')

        names = []
        
        for n in node.table:
            i = self.get_inode(n)

            if i.type == 0:
                names.append(f"{bcolors.OKBLUE}{i.name}{bcolors.ENDC}")
            elif i.type == 1:
                names.append(i.name)
        
        print(" ".join(names))
    
    def mvdir(self, origin, destiny):
        # move um diretório para outro
        (origin_address, parent_address) = self._resolvePath(origin)
        parent_address = parent_address[-2]
        destiny_address = self._resolvePath(destiny)[0]

        orig = self.get_inode(origin_address)
        par = self.get_inode(parent_address)
        dest = self.get_inode(destiny_address)

        if len(par.table) == 1962:
            raise Exception('Folder is full, it doesn\'t support more iNodes.')

        if par.type != 0:
            raise Exception(f'{par.name} is not a directory')

        # checa se dir já existe no dir destino
        (has, pos) = self._get_subdir(dest.table, orig.name)
        if has:
            raise Exception(f"Directory {destiny}/{orig.name} already exists")

        # insere o endereço na tabela do dir destino
        dest.table.insert(pos, origin_address)

        # remove do parent da origem
        (has, pos) = self._get_subdir(par.table, orig.name)
        par.table.pop(pos)

        dest.modified = int(datetime.datetime.now().timestamp())

        # atualiza parent e destino em disco
        self.set_inode(parent_address, par)
        self.set_inode(destiny_address, dest)

    def mv(self, where, name):
        # renomeia um arquivo ou diretorio
        address, parent_address = self._resolvePath(where)
        
        if address == self.root:
            raise Exception("You can\'t rename your root directory")
        
        parent_address = parent_address[-2]

        par = self.get_inode(parent_address)
        node = self.get_inode(address)

        # checa se já existe um node com o mesmo nome
        (has, pos) = self._get_subdir(par.table, name)
        if has:
            d = "/".join(where.split("/")[:-1]) + name
            raise Exception(f"{d} already exists!")

        # retira o node com nome antigo da tabela
        (has, pos) = self._get_subdir(par.table, node.name)
        par.table.pop(pos)

        # insere com novo nome
        (has, pos) = self._get_subdir(par.table, name)
        par.table.insert(pos, address)

        # atualiza em disco
        node.name = name
        self.set_inode(address, node)
        self.set_inode(parent_address, par)

    def touch(self, where, name):
        if '/' in name:
            raise Exception('Bad file name')

        parent = self.get_inode(where)

        (has, idx) = self._get_subdir(parent.table, name)
 
        if has:
            raise Exception('File already exists')

        new_file = iNode(name, 1, datetime.datetime.now().timestamp(), datetime.datetime.now().timestamp(), self.user, [])
        file_idx = self._allocate()
        self.set_inode(file_idx, new_file)
        parent.table.insert(idx, file_idx)
        self.set_inode(where, parent)

    def rm(self, where, name):
        parent = self.get_inode(where)

        (has, idx) = self._get_subdir(parent.table, name)

        if not has or self.get_inode(parent.table[idx]).type != 1:
            raise Exception('File doesn\'t exist')

        # TO DO: FREE WRITTEN BLOCKS
        self._deallocate(parent.table[idx])
        parent.table.pop(idx)
        self.set_inode(where, parent)

    def run(self):
        while True:
            # get user input
            curr_path = [self.get_inode(i).name for i in self.current_dir]
            print(f"{bcolors.BOLD}{bcolors.OKGREEN}{self.user}{bcolors.ENDC}{bcolors.ENDC}@{bcolors.BOLD}{bcolors.OKBLUE}{'/'.join(curr_path)}{bcolors.ENDC}{bcolors.ENDC}$ ", end='', flush=True)

            if self.current_dir:
                curr_dir = self.current_dir[-1]
            else:
                curr_dir = self.root
            
            try:
                usr_inp = input().split(" ")
            except KeyboardInterrupt:
                print(" Bye!")
                return
            
            # print(usr_inp)
            command = usr_inp[0]

            try:
                if command == 'mkdir':
                    self.mkdir(usr_inp[1])

                elif command == 'rmdir':
                    self.rmdir(curr_dir, usr_inp[1])

                elif command == 'mvdir':
                    if len(usr_inp) != 3:
                        raise Exception('Bad arguments.')
                        
                    fr, to = usr_inp[1:3]
                    self.mvdir(fr, to)

                elif command == 'cd':
                    paths = self._resolvePath(usr_inp[1])
                    self.current_dir = paths[1]
                
                elif command == 'mv':
                    self.mv(usr_inp[1], usr_inp[2])


                elif command == 'ls':
                    self.ls(curr_dir)

                elif command == 'touch':
                    self.touch(curr_dir, usr_inp[1])

                elif command == 'rm':
                    self.rm(curr_dir, usr_inp[1])

                else:
                    pass

            except Exception as e:
                print(command, e.with_traceback())

            
def test():
    A = DiskManager('disk.bin', wipeDisk=True, user='victor')
    A.run()

if __name__ == "__main__":
    test()
    pass

# REQUIREMENTS:

# Operações sobre arquivos:

#     - Criar arquivo (touch arquivo)
#     - Remover arquivo (rm arquivo)
#     - Escrever no arquivo (echo "conteudo legal" >> arquivo)
#     - Ler arquivo (cat arquivo)
#     - Copiar arquivo (cp arquivo1 arquivo2)
#     - Renomear arquivo (mv arquivo1 arquivo2)

# Operações sobre diretórios:

#     - Criar diretório (mkdir diretorio)
#     - Remover diretório (rmdir diretorio) - só funciona se diretório estiver vazio
#     - Trocar de diretório (cd diretorio)
#         * Não esquecer dos arquivos especiais . e .. 
#     - Renomear diretorio (mv diretorio1 diretorio2)