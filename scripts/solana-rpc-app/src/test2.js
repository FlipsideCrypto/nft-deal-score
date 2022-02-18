"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var web3_js_1 = require("@solana/web3.js");
// import bs58 from 'bs58';
var connection = new web3_js_1.Connection(web3_js_1.clusterApiUrl('mainnet-beta'));
var MAX_NAME_LENGTH = 32;
var MAX_URI_LENGTH = 200;
var MAX_SYMBOL_LENGTH = 10;
var MAX_CREATOR_LEN = 32 + 1 + 1;
var MAX_CREATOR_LIMIT = 5;
var MAX_DATA_SIZE = 4 + MAX_NAME_LENGTH + 4 + MAX_SYMBOL_LENGTH + 4 + MAX_URI_LENGTH + 2 + 1 + 4 + MAX_CREATOR_LIMIT * MAX_CREATOR_LEN;
var MAX_METADATA_LEN = 1 + 32 + 32 + MAX_DATA_SIZE + 1 + 1 + 9 + 172;
var CREATOR_ARRAY_START = 1 + 32 + 32 + 4 + MAX_NAME_LENGTH + 4 + MAX_URI_LENGTH + 4 + MAX_SYMBOL_LENGTH + 2 + 1 + 4;
// const TOKEN_METADATA_PROGRAM = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');
var TOKEN_METADATA_PROGRAM = new web3_js_1.PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
var candyMachineId = new web3_js_1.PublicKey('8mNmf15xNrMFQLNSNrHxxswy7a1NfaSFwXHkVUPeMWwU');
var getMintAddresses = function (firstCreatorAddress) { return __awaiter(void 0, void 0, void 0, function () {
    var metadataAccounts;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, connection.getProgramAccounts(TOKEN_METADATA_PROGRAM, {
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
                                bytes: new web3_js_1.PublicKey('4FYjfa71puV4PD12cyqXotu6z2FhLiqFSHjEfYiFLnbj').toBase58()
                            }
                        },
                    ]
                })];
            case 1:
                metadataAccounts = _a.sent();
                return [2 /*return*/, metadataAccounts];
        }
    });
}); };
(function () { return __awaiter(void 0, void 0, void 0, function () {
    var a;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, getMintAddresses(candyMachineId)];
            case 1:
                a = _a.sent();
                console.log("a");
                console.log(a);
                console.log(a.length);
                return [2 /*return*/];
        }
    });
}); })();
