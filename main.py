import mmap
import datetime
from math import log, log2
import re

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

    def __init__(self, diskpath, wipeDisk = False) -> None:

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

        self.root = 2
        self.current_dir = []

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
    
    def set_inode(self, idx, inode):
        self._writeBytes(idx, inode.toBytes())

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

    def mkdir(self, where, name):
        if len(where.table) == 1962:
            raise Exception('Folder is full, it doesn\'t support more iNodes.')

        new_dir = self._get_subdir(where.table, name)

        if new_dir[0] == True:
            print(f'Directory "{name}" already exists')
            return
        
        new_dir_block = self._allocate() # aloca um novo inode

        where.table.insert(new_dir[1], new_dir_block)
        self.set_inode(new_dir_block * BLOCKSIZE, iNode(name, 0, datetime.datetime.now().timestamp(), datetime.datetime.now().timestamp(), 'system'))

    def rmdir(self, where, name):
        check_dir = self._get_subdir(where.table, name)
        
        if check_dir[0] == False:
            print(f'Directory "{name}" does not exist')
            return

        dir_idx = where.table[check_dir[1]]
        dir_inode = self.get_inode(dir_idx)

        if len(dir_inode.table) > 0:
            print(f'Directory "{name}" is not empty')
            return
        
        self._deallocate(where.table[check_dir[1]])
        where.table.pop(check_dir[1])

    def _resolvePath(self, pathString):
        curr_path = self.current_dir.copy()
        tokens = pathString.split("/")
        
        for t in tokens:
            
            if len(curr_path) > 0:
                curr_node = curr_path[-1]
            else:
                curr_node = self.root
            curr_node = self.get_inode(curr_node)

            if t == '..' and len(curr_path) > 0:
                curr_path.pop()
                continue
            elif t == '.':
                continue

            if curr_node.type != 0:
                raise FileNotFoundError(f'{curr_node.name} is not a directory')

            (has, idx) = self._get_subdir(curr_node.table, t)
            if not has:
                raise FileNotFoundError(f"{curr_node.name}/{t} doens\'t exist")
            else:
                curr_path.append(idx)
                
        if len(curr_path) > 0:
            return (idx, curr_path)
        else:
            return (self.root, curr_path)


    
    def run(self):
        while True:
            # get user input
            curr_path = [self.get_inode(self.root).name] + [self.get_inode(i).name for i in self.current_dir]
            print(f"{bcolors.OKBLUE}{'/'.join(curr_path)}{bcolors.ENDC}$ ", end='', flush=True)
            usr_inp = input()


        
        blocks = idx * BLOCKSIZE
        return iNode.fromBytes(self._readBytes(blocks, blocks + BLOCKSIZE))



def test():
    A = DiskManager('disk.bin', wipeDisk=True)
    
    curr_dir = A.get_inode(2)
    A.mkdir(curr_dir, 'kek')
    A.mkdir(curr_dir, 'kekw')
    print('\nFolders: ')
    for subdir in curr_dir.table:
        i = A.get_inode(subdir)
        print(i)

    kek_dir = A.get_inode(3)
    A.mkdir(kek_dir, 'lol')
    A.set_inode(3 * BLOCKSIZE, kek_dir)
    print('kek_dir', kek_dir)
    A.rmdir(curr_dir, 'kek')

    print('\nFolders: ')
    for subdir in curr_dir.table:
        i = A.get_inode(subdir)
        print(i)

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