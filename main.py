import mmap
import datetime
from math import log, log2

DISKSIZE = 128*(2**20)
BLOCKSIZE = 4*(2**10)
BLOCKNUMBER = int(DISKSIZE/BLOCKSIZE)

# iterator sobre bits
def bits(int):
    mask = 0b10000000
    for i in range(8):
        if int & mask:
            yield (False, i)
        else:
            yield (True, i)
        mask = mask >> 1



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
            byteblock[0:128].decode('utf-8'),
            int.from_bytes(byteblock[128:130], 'big', signed=False),
            int.from_bytes(byteblock[130:134], 'big', signed=False),
            int.from_bytes(byteblock[134:138], 'big', signed=False),
            byteblock[138:168].decode('utf-8'),
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
    
    
        

def test():
    A = DiskManager('disk.bin', wipeDisk=False)
    a = A._allocate()
    print(a)
    # A._deallocate(i)

    # print(iNode.fromBytes(A._readBytes(A.INODESTART, A.INODESTART + BLOCKSIZE)))
    pass


if __name__ == "__main__":
    test()
    pass
