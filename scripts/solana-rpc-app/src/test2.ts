import { Connection, clusterApiUrl, PublicKey } from '@solana/web3.js';
// import bs58 from 'bs58';

const connection = new Connection(clusterApiUrl('mainnet-beta'));
const MAX_NAME_LENGTH = 32;
const MAX_URI_LENGTH = 200;
const MAX_SYMBOL_LENGTH = 10;
const MAX_CREATOR_LEN = 32 + 1 + 1;
const MAX_CREATOR_LIMIT = 5;
const MAX_DATA_SIZE = 4 + MAX_NAME_LENGTH + 4 + MAX_SYMBOL_LENGTH + 4 + MAX_URI_LENGTH + 2 + 1 + 4 + MAX_CREATOR_LIMIT * MAX_CREATOR_LEN;
const MAX_METADATA_LEN = 1 + 32 + 32 + MAX_DATA_SIZE + 1 + 1 + 9 + 172;
const CREATOR_ARRAY_START = 1 + 32 + 32 + 4 + MAX_NAME_LENGTH + 4 + MAX_URI_LENGTH + 4 + MAX_SYMBOL_LENGTH + 2 + 1 + 4;

// const TOKEN_METADATA_PROGRAM = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');
const TOKEN_METADATA_PROGRAM = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const candyMachineId = new PublicKey('8mNmf15xNrMFQLNSNrHxxswy7a1NfaSFwXHkVUPeMWwU');

const getMintAddresses = async (firstCreatorAddress: PublicKey) => {
  const metadataAccounts = await connection.getProgramAccounts(
    TOKEN_METADATA_PROGRAM,
    {
      // The mint address is located at byte 33 and lasts for 32 bytes.
    //   dataSlice: { offset: 33, length: 32 },

      filters: [
        // Only get Metadata accounts.
        // { dataSize: MAX_METADATA_LEN },
        { dataSize: 165 },

        // Filter using the first creator.
        {
          memcmp: {
            // offset: CREATOR_ARRAY_START,
            // bytes: firstCreatorAddress.toBase58(),
            offset: 1,
            bytes: new PublicKey('4FYjfa71puV4PD12cyqXotu6z2FhLiqFSHjEfYiFLnbj').toBase58(),
          },
        },
      ],
    },
  );
  return metadataAccounts;

//   return metadataAccounts.map((metadataAccountInfo) => (
//     bs58.encode(metadataAccountInfo.account.data)
//   ));
};


(async () => {

      const a = await getMintAddresses(candyMachineId);
      console.log(`a`);
      console.log(a);
      console.log(a.length);
    
    })();