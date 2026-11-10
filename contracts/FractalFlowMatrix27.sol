// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IERC20 {
    function totalSupply() external view returns (uint256);
    function balanceOf(address account) external view returns (uint256);
    function transfer(address to, uint256 value) external returns (bool);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 value) external returns (bool);
    function transferFrom(address from, address to, uint256 value) external returns (bool);
}

library SafeERC20 {
    error SafeERC20FailedOperation(address token);

    function safeTransfer(IERC20 token, address to, uint256 value) internal {
        _callOptionalReturn(token, abi.encodeCall(token.transfer, (to, value)));
    }

    function safeTransferFrom(IERC20 token, address from, address to, uint256 value) internal {
        _callOptionalReturn(token, abi.encodeCall(token.transferFrom, (from, to, value)));
    }

    function _callOptionalReturn(IERC20 token, bytes memory data) private {
        (bool success, bytes memory returndata) = address(token).call(data);
        if (!success || (returndata.length != 0 && !abi.decode(returndata, (bool)))) {
            revert SafeERC20FailedOperation(address(token));
        }
    }
}

abstract contract Ownable {
    address private _owner;

    error OwnableUnauthorizedAccount(address account);
    error OwnableInvalidOwner(address owner);

    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);

    constructor(address initialOwner) {
        if (initialOwner == address(0)) revert OwnableInvalidOwner(address(0));
        _transferOwnership(initialOwner);
    }

    modifier onlyOwner() {
        if (owner() != msg.sender) revert OwnableUnauthorizedAccount(msg.sender);
        _;
    }

    function owner() public view virtual returns (address) {
        return _owner;
    }

    function renounceOwnership() public virtual onlyOwner {
        _transferOwnership(address(0));
    }

    function transferOwnership(address newOwner) public virtual onlyOwner {
        if (newOwner == address(0)) revert OwnableInvalidOwner(address(0));
        _transferOwnership(newOwner);
    }

    function _transferOwnership(address newOwner) internal virtual {
        address oldOwner = _owner;
        _owner = newOwner;
        emit OwnershipTransferred(oldOwner, newOwner);
    }
}

abstract contract ReentrancyGuard {
    uint256 private _status = 1;
    error ReentrancyGuardReentrantCall();

    modifier nonReentrant() {
        if (_status == 2) revert ReentrancyGuardReentrantCall();
        _status = 2;
        _;
        _status = 1;
    }
}

contract FractalFlowMatrix27 is Ownable, ReentrancyGuard {
    using SafeERC20 for IERC20;

    IERC20 public immutable usdt;
    address public allSellWallet;
    address public botWallet;
    address public familyPool;

    uint8 public constant MAX_RANK = 27;
    uint8 public constant MAX_LEVELS = 15;

    mapping(uint8 => uint256) public rankPrice;

    struct MatrixNode {
        uint256 id;
        address owner;
        address directSponsor;
        uint256 parentId;
        uint256 leftChild;
        uint256 rightChild;
        bool isClone;
        bool isExtraId;
    }

    struct UserAccount {
        bool isRegistered;
        address sponsor;
        uint256 sponsorMainNodeId;
        uint8 currentMaxRank;
        mapping(uint8 => uint256) firstNodeId;
        mapping(uint8 => uint256) latestNodeId;
        mapping(uint8 => uint256) upgradeVault;
    }

    mapping(address => UserAccount) public users;
    mapping(uint8 => mapping(uint256 => MatrixNode)) public rankNodes;
    mapping(uint8 => uint256) public totalNodesInRank;
    mapping(uint8 => mapping(uint256 => uint8)) public sponsorRoundToggle;
    mapping(uint8 => uint8) public placementRoundToggle;
    mapping(uint8 => mapping(uint256 => uint256)) public nodeOriginMainId;
    mapping(uint8 => mapping(uint256 => uint256)) public nodeDirectSponsorNodeId;
    mapping(uint8 => mapping(uint256 => uint256)) public mainNodeLatestCloneId;
    mapping(uint8 => mapping(uint256 => uint256)) public nodeUpgradeVault;
    mapping(uint8 => mapping(uint256 => uint8)) public nodeCloneRound;
    mapping(uint8 => mapping(uint256 => uint256)) public nodeCloneTotalCount;
    mapping(uint256 => uint8) public mainIdCurrentMaxRank;
    mapping(uint256 => mapping(uint8 => uint256)) public mainIdNodeInRank;
    mapping(uint8 => mapping(uint256 => uint256)) public nodeToR1MainId;
    mapping(uint256 => uint256) public mainIdExtraPoolBonus;
    mapping(uint256 => uint8) public mainIdNextExtraRank;

    struct PendingCloneItem {
        uint8 rank;
        address parentOwner;
        address parentSponsor;
        uint256 cloneOriginMainId;
        uint256 targetSponsorNodeId;
        uint256 r1SponsorId;
    }

    PendingCloneItem[] public pendingCloneQueue;
    uint256 public cloneQueueHead;

    error ErrInvalidSponsor();
    error ErrNotOwner();
    error ErrInvalidRank();
    error ErrStepByStep();
    error ErrRankActive();
    error ErrBadParam();
    error ErrZero();
    error ErrNotRegistered();

    event UserRegistered(address indexed user, address indexed sponsor, uint256 sponsorNodeId, uint256 newMainNodeId);
    event NodeCreated(
        uint8 indexed rank,
        uint256 indexed nodeId,
        address indexed owner,
        address sponsor,
        uint256 parentId,
        bool isLeftChild,
        bool isClone,
        bool isExtraId
    );
    event RewardDistributed(
        uint256 indexed fromNodeId,
        uint256 indexed toNodeId,
        address indexed toAddress,
        uint8 rank,
        string rewardType,
        uint256 amount
    );
    event RankUpgraded(address indexed user, uint8 fromRank, uint8 toRank, uint256 newNodeId, bool isAutoUpgrade);
    event AutoExtraIdCreated(address indexed user, uint8 rank, uint256 newNodeId);
    event AutoNewMainIdCreated(address indexed user, uint256 count, uint256 totalUsedFromVault);
    event BatchRegistered(address indexed user, uint256 count, uint256 totalPaid);
    event CloneQueued(uint8 indexed rank, uint256 indexed originMainId, uint256 targetSponsorNodeId);
    event CloneQueueProcessed(uint256 processedCount, uint256 remainingCount);

    constructor(
        address _usdt,
        uint8 _usdtDecimals,
        address _allSellWallet,
        address _botWallet,
        address _familyPool,
        address _rootUser
    ) Ownable(msg.sender) {
        if (_usdt == address(0) || _rootUser == address(0)) revert ErrZero();

        usdt = IERC20(_usdt);
        allSellWallet = _allSellWallet;
        botWallet = _botWallet;
        familyPool = _familyPool;

        uint256 unit = 10 ** _usdtDecimals;
        uint16[27] memory prices = [
            5, 10, 20, 30, 40, 50, 60, 70, 80, 90,
            100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
            2000, 3000, 4000, 5000, 6000, 8000, 10000
        ];

        for (uint8 i = 0; i < MAX_RANK;) {
            rankPrice[i + 1] = uint256(prices[i]) * unit;
            unchecked { ++i; }
        }

        users[_rootUser].isRegistered = true;
        users[_rootUser].currentMaxRank = MAX_RANK;
        mainIdCurrentMaxRank[1] = MAX_RANK;
        mainIdNextExtraRank[1] = 2;

        for (uint8 r = 1; r <= MAX_RANK;) {
            totalNodesInRank[r] = 1;
            rankNodes[r][1] = MatrixNode({
                id: 1,
                owner: _rootUser,
                directSponsor: address(0),
                parentId: 0,
                leftChild: 0,
                rightChild: 0,
                isClone: false,
                isExtraId: false
            });
            users[_rootUser].firstNodeId[r] = 1;
            users[_rootUser].latestNodeId[r] = 1;
            nodeOriginMainId[r][1] = 1;
            nodeDirectSponsorNodeId[r][1] = 1;
            mainIdNodeInRank[1][r] = 1;
            nodeToR1MainId[r][1] = 1;

            emit NodeCreated(r, 1, _rootUser, address(0), 0, true, false, false);
            unchecked { ++r; }
        }
    }

    function checkValidSponsorId(uint256 _sponsorNodeId)
        public
        view
        returns (bool isValid, string memory reason, address sponsorAddress)
    {
        if (_sponsorNodeId == 0 || _sponsorNodeId > totalNodesInRank[1]) {
            return (false, "Not found", address(0));
        }
        MatrixNode storage sponsorNode = rankNodes[1][_sponsorNodeId];
        if (sponsorNode.owner == address(0)) {
            return (false, "No owner", address(0));
        }
        if (sponsorNode.isClone || sponsorNode.isExtraId) {
            return (false, "Special node", address(0));
        }
        return (true, "Valid", sponsorNode.owner);
    }

    function previewPlacement(uint8 _rank, address _sponsor)
        public
        view
        returns (uint256 targetParentId, bool isLeft, uint8 activeRound)
    {
        uint256 sponsorNodeId = users[_sponsor].firstNodeId[_rank];
        activeRound = sponsorNodeId > 0 ? sponsorRoundToggle[_rank][sponsorNodeId] : 0;
        (targetParentId, isLeft) = _findPlacementSpot(_rank, _sponsor, sponsorNodeId);
    }

    function previewPlacement(uint8 _rank, uint256 _sponsorNodeId)
        external
        view
        returns (uint256 targetParentId, bool isLeft, uint8 activeRound)
    {
        (bool isValid, , address sponsorAddr) = checkValidSponsorId(_sponsorNodeId);
        if (!isValid) revert ErrInvalidSponsor();
        return previewPlacement(_rank, sponsorAddr);
    }

    function getFiveRankWindow(uint8 _currentRank) public pure returns (uint8 window5Start, uint8 window5End) {
        uint8 safeRank = _currentRank == 0 ? 1 : (_currentRank > MAX_RANK ? MAX_RANK : _currentRank);
        window5Start = safeRank >= 5 ? safeRank - 4 : 1;
        window5End = safeRank;
    }

    function getMainIdCurrentRank(uint256 _r1MainId) public view returns (uint8) {
        if (_r1MainId == 1) return MAX_RANK;
        uint8 r = mainIdCurrentMaxRank[_r1MainId];
        if (r > 0) return r;
        address nodeOwner = rankNodes[1][_r1MainId].owner;
        if (nodeOwner != address(0) && users[nodeOwner].firstNodeId[1] == _r1MainId && users[nodeOwner].currentMaxRank > 0) {
            return users[nodeOwner].currentMaxRank;
        }
        return 1;
    }

    function getMainIdNodeInRank(uint256 _r1MainId, uint8 _rank) public view returns (uint256) {
        if (_r1MainId == 1) return 1;
        if (_r1MainId >= 1_000_000) {
            return _rank == 1 ? 0 : mainIdNodeInRank[_r1MainId][_rank];
        }
        if (_rank == 1) return _r1MainId;
        return mainIdNodeInRank[_r1MainId][_rank];
    }

    function getAccumulatedVaultForMainId(uint256 _r1MainId) public view returns (uint256 totalVault) {
        uint8 currentRank = getMainIdCurrentRank(_r1MainId);
        (uint8 startRank, uint8 endRank) = getFiveRankWindow(currentRank);
        for (uint8 r = startRank; r <= endRank;) {
            uint256 nId = getMainIdNodeInRank(_r1MainId, r);
            if (nId > 0) totalVault += nodeUpgradeVault[r][nId];
            unchecked { ++r; }
        }
    }

    function getAccumulatedVault(address _user, uint8 _targetRank) public view returns (uint256 totalVault) {
        uint256 r1MainId = users[_user].firstNodeId[1];
        if (r1MainId > 0) return getAccumulatedVaultForMainId(r1MainId);
        uint8 currentRank = _targetRank > 1 ? _targetRank - 1 : 1;
        (uint8 startRank, uint8 endRank) = getFiveRankWindow(currentRank);
        for (uint8 r = startRank; r <= endRank && r < _targetRank;) {
            totalVault += users[_user].upgradeVault[r];
            unchecked { ++r; }
        }
    }

    function _deductVaultRange(
        uint256 _r1MainId,
        address _owner,
        uint8 _start,
        uint8 _end,
        uint256 _amount
    ) internal {
        uint256 rem = _amount;
        for (uint8 r = _start; r <= _end && rem > 0;) {
            uint256 nId = getMainIdNodeInRank(_r1MainId, r);
            if (nId != 0) {
                uint256 bal = nodeUpgradeVault[r][nId];
                if (bal > 0) {
                    uint256 deduct = bal <= rem ? bal : rem;
                    rem -= deduct;
                    nodeUpgradeVault[r][nId] = bal - deduct;
                    if (_owner != address(0)) {
                        uint256 uBal = users[_owner].upgradeVault[r];
                        users[_owner].upgradeVault[r] = uBal <= deduct ? 0 : uBal - deduct;
                    }
                }
            }
            unchecked { ++r; }
        }
    }

    function getMainIdThreePools(address _user, uint256 _mainNodeIdRank1)
        public
        view
        returns (
            uint8 currentRank,
            uint8 window5Start,
            uint8 window5End,
            uint256 upgradeRankPool,
            uint256 newAutoIdPool,
            uint256 newExtraIdPool
        )
    {
        uint256 targetMainId = _mainNodeIdRank1 > 0 ? _mainNodeIdRank1 : users[_user].firstNodeId[1];
        currentRank = getMainIdCurrentRank(targetMainId);
        (window5Start, window5End) = getFiveRankWindow(currentRank);

        for (uint8 r = window5Start; r <= window5End;) {
            uint256 nId = getMainIdNodeInRank(targetMainId, r);
            if (nId > 0) upgradeRankPool += nodeUpgradeVault[r][nId];
            unchecked { ++r; }
        }

        for (uint8 r = 1; r <= 3;) {
            if (r < window5Start) {
                uint256 nId = getMainIdNodeInRank(targetMainId, r);
                if (nId > 0) newAutoIdPool += nodeUpgradeVault[r][nId];
            }
            unchecked { ++r; }
        }

        newExtraIdPool = mainIdExtraPoolBonus[targetMainId];
        for (uint8 r = 4; r <= MAX_RANK;) {
            if (currentRank == MAX_RANK || r < window5Start) {
                uint256 nId = getMainIdNodeInRank(targetMainId, r);
                if (nId > 0) newExtraIdPool += nodeUpgradeVault[r][nId];
            }
            unchecked { ++r; }
        }
    }

    function getNodeVault(uint8 _rank, uint256 _nodeId) external view returns (uint256) {
        return nodeUpgradeVault[_rank][_nodeId];
    }

    function getPendingCloneCount() external view returns (uint256) {
        uint256 total = pendingCloneQueue.length;
        uint256 head = cloneQueueHead;
        return total > head ? total - head : 0;
    }

    function processCloneQueue(uint256 _maxIterations) external nonReentrant {
        uint256 total = pendingCloneQueue.length;
        uint256 head = cloneQueueHead;
        if (total <= head || _maxIterations == 0) return;

        uint256 available = total - head;
        uint256 iterations = _maxIterations < available ? _maxIterations : available;

        for (uint256 i = 0; i < iterations;) {
            uint256 idx = head + i;
            PendingCloneItem memory item = pendingCloneQueue[idx];
            if (item.parentOwner != address(0)) {
                _spawnCloneNode(item.rank, item.parentOwner, item.parentSponsor, item.cloneOriginMainId, item.targetSponsorNodeId, true);
                delete pendingCloneQueue[idx];
            }
            unchecked { ++i; }
        }

        head += iterations;
        cloneQueueHead = head;
        if (head >= pendingCloneQueue.length) {
            delete pendingCloneQueue;
            cloneQueueHead = 0;
            emit CloneQueueProcessed(iterations, 0);
        } else {
            emit CloneQueueProcessed(iterations, pendingCloneQueue.length - head);
        }
    }

    function checkAndProcessAutoNewMainId(address _user) public {
        uint256 r1MainId = users[_user].firstNodeId[1];
        if (r1MainId > 0) checkAndProcessAutoNewMainIdForNode(r1MainId);
    }

    function checkAndProcessAutoNewMainIdForNode(uint256 _mainNodeId) public {
        if (_mainNodeId == 0 || _mainNodeId > totalNodesInRank[1]) revert ErrBadParam();
        address nodeOwner = rankNodes[1][_mainNodeId].owner;
        if (nodeOwner == address(0)) revert ErrZero();

        (, , , , uint256 autoIdPool, ) = getMainIdThreePools(nodeOwner, _mainNodeId);
        uint256 r1Price = rankPrice[1];
        if (r1Price > 0 && autoIdPool >= r1Price) {
            uint256 count = autoIdPool / r1Price;
            if (count > 2) count = 2;
            uint256 totalDeduct = count * r1Price;
            _deductVaultRange(_mainNodeId, nodeOwner, 1, 3, totalDeduct);
            for (uint256 i = 0; i < count;) {
                _insertNode(1, nodeOwner, nodeOwner, _mainNodeId, 0, true, false, false, true);
                unchecked { ++i; }
            }
            emit AutoNewMainIdCreated(nodeOwner, count, totalDeduct);
        }
    }

    function checkAndProcessAutoExtraId(address _user, uint8 _currentRank) public {
        uint256 mainNodeId = users[_user].firstNodeId[1];
        if (mainNodeId > 0) processPendingExtraIdQueue(mainNodeId, 1);
    }

    function checkAndProcessAutoExtraIdForNode(address, uint8, uint256 _sponsorNodeId) public {
        if (_sponsorNodeId > 0) processPendingExtraIdQueue(_sponsorNodeId, 1);
    }

    function processPendingExtraIdQueue(uint256 _r1MainId, uint8 _maxBatch) public returns (uint8 processed) {
        if (_r1MainId == 0 || _r1MainId > totalNodesInRank[1]) return 0;
        address nodeOwner = rankNodes[1][_r1MainId].owner;
        if (nodeOwner == address(0)) return 0;

        (uint8 mainMaxRank, uint8 window5Start, , , , uint256 pool) = getMainIdThreePools(nodeOwner, _r1MainId);
        uint8 limit = _maxBatch == 0 || _maxBatch > 2 ? 2 : _maxBatch;

        for (uint8 pass = 0; pass < limit;) {
            bool spawned = false;
            for (uint8 targetRank = MAX_RANK; targetRank >= 2;) {
                uint256 price = rankPrice[targetRank];
                if (price != 0 && pool >= price && getMainIdNodeInRank(_r1MainId, targetRank) != 0) {
                    uint8 endR = mainMaxRank == MAX_RANK ? MAX_RANK : (window5Start > 4 ? window5Start - 1 : 4);
                    _deductVaultRange(_r1MainId, nodeOwner, 4, endR, price);
                    pool -= price;
                    uint256 spInTarget = getMainIdNodeInRank(_r1MainId, targetRank);
                    uint256 extraId = _insertNode(targetRank, nodeOwner, nodeOwner, spInTarget, 0, true, false, true, true);
                    emit AutoExtraIdCreated(nodeOwner, targetRank, extraId);
                    spawned = true;
                    unchecked { ++processed; }
                    break;
                }
                unchecked { --targetRank; }
            }
            if (!spawned) break;
            unchecked { ++pass; }
        }
    }

    function _ensureRegistered(address _user, address _sponsorAddr, uint256 _sponsorNodeId) internal {
        if (!users[_user].isRegistered) {
            users[_user].isRegistered = true;
            users[_user].sponsor = _sponsorAddr;
            users[_user].sponsorMainNodeId = _sponsorNodeId;
            users[_user].currentMaxRank = 1;
        }
    }

    function _creditVault(uint8 _rank, uint256 _fromNodeId, uint256 _targetNodeId, uint256 _amount) internal {
        uint256 vNodeId = _targetNodeId > 0 && _targetNodeId <= totalNodesInRank[_rank] ? _targetNodeId : 1;
        if (rankNodes[_rank][vNodeId].isClone && nodeOriginMainId[_rank][vNodeId] > 0) {
            uint256 origId = nodeOriginMainId[_rank][vNodeId];
            if (origId <= totalNodesInRank[_rank]) {
                vNodeId = origId;
            }
        }
        address vOwner = rankNodes[_rank][vNodeId].owner;
        if (vOwner != address(0)) {
            users[vOwner].upgradeVault[_rank] += _amount;
        }
        nodeUpgradeVault[_rank][vNodeId] += _amount;
        emit RewardDistributed(_fromNodeId, vNodeId, vOwner, _rank, "UPGRADE_VAULT", _amount);

        uint256 r1MainId = _rank == 1 ? vNodeId : nodeOriginMainId[_rank][vNodeId];
        if (r1MainId == 0) r1MainId = 1;
        if (gasleft() > 350000) {
            if (_rank <= 3) {
                checkAndProcessAutoNewMainIdForNode(r1MainId);
            }
            if (gasleft() > 350000) {
                processPendingExtraIdQueue(r1MainId, 1);
            }
        }
    }

    /**
     * @notice จ่ายค่าคอมมิชชันไปยังกระเป๋าผู้รับที่ระบบหน้าบ้านคำนวณส่งมาให้โดยตรง และบันทึกยอดสะสม Upgrade Vault
     */
    function _settleFrontendPayouts(
        uint8 _rank,
        uint256 _fromNodeId,
        address[] calldata _recipients,
        uint256[] calldata _amounts,
        uint256 _vaultTargetNodeId,
        uint256 _vaultAmount,
        uint256 _maxBudget
    ) internal {
        uint256 len = _recipients.length;
        if (len != _amounts.length) revert ErrBadParam();
        uint256 used = _vaultAmount;
        if (used > _maxBudget) revert ErrBadParam();

        for (uint256 i = 0; i < len;) {
            uint256 amt = _amounts[i];
            address recip = _recipients[i];
            if (amt > 0 && recip != address(0)) {
                used += amt;
                if (used > _maxBudget) revert ErrBadParam();
                usdt.safeTransfer(recip, amt);
                emit RewardDistributed(_fromNodeId, 0, recip, _rank, "COMMISSION", amt);
            }
            unchecked { ++i; }
        }

        if (_vaultAmount > 0) {
            _creditVault(_rank, _fromNodeId, _vaultTargetNodeId, _vaultAmount);
        }
    }

    /**
     * @notice สมัครสมาชิก Rank 1 พร้อมรับรายการกระเป๋าผู้รับค่าคอมมิชชันจากหน้าบ้านโดยตรง
     */
    function registerWithPayouts(
        uint256 _sponsorNodeId,
        uint256 _targetParentId,
        bool _isLeft,
        address[] calldata _recipients,
        uint256[] calldata _amounts,
        uint256 _vaultTargetNodeId,
        uint256 _vaultAmount
    ) external nonReentrant {
        (bool isValid, , address sponsorAddr) = checkValidSponsorId(_sponsorNodeId);
        if (!isValid) revert ErrInvalidSponsor();

        uint256 r1Price = rankPrice[1];
        usdt.safeTransferFrom(msg.sender, address(this), r1Price);
        _ensureRegistered(msg.sender, sponsorAddr, _sponsorNodeId);

        bool hasPayouts = _recipients.length > 0 || _vaultAmount > 0;
        uint256 newNodeId = _insertNode(1, msg.sender, sponsorAddr, _sponsorNodeId, _targetParentId, _isLeft, false, false, !hasPayouts);

        if (hasPayouts) {
            _settleFrontendPayouts(1, newNodeId, _recipients, _amounts, _vaultTargetNodeId, _vaultAmount, r1Price);
        }

        emit UserRegistered(msg.sender, sponsorAddr, _sponsorNodeId, newNodeId);
    }

    /**
     * @notice สมัครสมาชิกแบบกลุ่ม (Batch) พร้อมรับรายการกระเป๋าผู้รับค่าคอมมิชชันจากหน้าบ้านโดยตรง
     */
    function registerBatchWithPayouts(
        uint256 _sponsorNodeId,
        uint256 _count,
        uint256[] calldata _targetParentIds,
        bool[] calldata _isLefts,
        bool[] calldata _isClones,
        address[] calldata _recipients,
        uint256[] calldata _amounts,
        uint256 _vaultTargetNodeId,
        uint256 _vaultAmount
    ) external nonReentrant {
        bool hasPayouts = _recipients.length > 0 || _vaultAmount > 0;
        (uint256 firstCreated, uint256 totalCost) = _executeBatchRegister(
            _sponsorNodeId,
            _count,
            _targetParentIds,
            _isLefts,
            _isClones,
            !hasPayouts
        );

        if (hasPayouts) {
            _settleFrontendPayouts(1, firstCreated, _recipients, _amounts, _vaultTargetNodeId, _vaultAmount, totalCost);
        }
    }

    function registerBatch(
        uint256 _sponsorNodeId,
        uint256 _count,
        uint256[] calldata _targetParentIds,
        bool[] calldata _isLefts,
        bool[] calldata _isClones
    ) external nonReentrant {
        _executeBatchRegister(_sponsorNodeId, _count, _targetParentIds, _isLefts, _isClones, true);
    }

    function _executeBatchRegister(
        uint256 _sponsorNodeId,
        uint256 _count,
        uint256[] calldata _targetParentIds,
        bool[] calldata _isLefts,
        bool[] calldata _isClones,
        bool _payOnChainRewards
    ) internal returns (uint256 firstCreated, uint256 totalCost) {
        if (_count == 0) revert ErrBadParam();
        (bool isValid, , address sponsorAddr) = checkValidSponsorId(_sponsorNodeId);
        if (!isValid) revert ErrInvalidSponsor();

        totalCost = rankPrice[1] * _count;
        usdt.safeTransferFrom(msg.sender, address(this), totalCost);
        _ensureRegistered(msg.sender, sponsorAddr, _sponsorNodeId);

        bool hasTargets = _targetParentIds.length == _count && _isLefts.length == _count && _isClones.length == _count;

        for (uint256 i = 0; i < _count;) {
            uint256 nId = _insertNode(
                1,
                msg.sender,
                sponsorAddr,
                _sponsorNodeId,
                hasTargets ? _targetParentIds[i] : 0,
                hasTargets ? _isLefts[i] : true,
                hasTargets ? _isClones[i] : false,
                false,
                _payOnChainRewards
            );
            if (firstCreated == 0) firstCreated = nId;
            unchecked { ++i; }
        }

        emit UserRegistered(msg.sender, sponsorAddr, _sponsorNodeId, firstCreated);
        emit BatchRegistered(msg.sender, _count, totalCost);
    }

    function register(uint256 _sponsorNodeId) external nonReentrant {
        (bool isValid, , address sponsorAddr) = checkValidSponsorId(_sponsorNodeId);
        if (!isValid) revert ErrInvalidSponsor();

        usdt.safeTransferFrom(msg.sender, address(this), rankPrice[1]);
        _ensureRegistered(msg.sender, sponsorAddr, _sponsorNodeId);

        uint256 newMainNodeId = _insertNode(1, msg.sender, sponsorAddr, _sponsorNodeId, 0, true, false, false, true);
        emit UserRegistered(msg.sender, sponsorAddr, _sponsorNodeId, newMainNodeId);
    }

    function _collectUpgradePayment(uint256 _r1MainId, uint8 _targetRank) internal returns (uint256 targetPrice) {
        if (_r1MainId == 0 || _r1MainId > totalNodesInRank[1]) revert ErrBadParam();
        MatrixNode storage mNode = rankNodes[1][_r1MainId];
        if (mNode.owner != msg.sender) revert ErrNotOwner();
        if (mNode.isClone || mNode.isExtraId) revert ErrBadParam();
        if (_targetRank <= 1 || _targetRank > MAX_RANK) revert ErrInvalidRank();

        uint8 currentRank = getMainIdCurrentRank(_r1MainId);
        if (_targetRank != currentRank + 1) revert ErrStepByStep();
        if (getMainIdNodeInRank(_r1MainId, _targetRank) != 0) revert ErrRankActive();

        targetPrice = rankPrice[_targetRank];
        uint256 savedInVault = getAccumulatedVaultForMainId(_r1MainId);
        (uint8 startRank, uint8 endRank) = getFiveRankWindow(currentRank);

        if (savedInVault >= targetPrice) {
            _deductVaultRange(_r1MainId, msg.sender, startRank, endRank, targetPrice);
        } else {
            if (savedInVault > 0) {
                _deductVaultRange(_r1MainId, msg.sender, startRank, endRank, savedInVault);
            }
            usdt.safeTransferFrom(msg.sender, address(this), targetPrice - savedInVault);
        }
    }

    /**
     * @notice อัปเกรด Rank พร้อมรับตำแหน่งวางและรายการกระเป๋าผู้รับค่าคอมมิชชันจากหน้าบ้านโดยตรง
     */
    function upgradeRankWithPayouts(
        uint256 _r1MainId,
        uint8 _targetRank,
        uint256 _targetParentId,
        bool _isLeft,
        address[] calldata _recipients,
        uint256[] calldata _amounts,
        uint256 _vaultTargetNodeId,
        uint256 _vaultAmount
    ) external nonReentrant {
        uint256 targetPrice = _collectUpgradePayment(_r1MainId, _targetRank);
        bool hasPayouts = _recipients.length > 0 || _vaultAmount > 0;
        uint256 newNodeId = _applyRankUpgrade(_r1MainId, msg.sender, _targetRank, false, _targetParentId, _isLeft, !hasPayouts);
        if (hasPayouts) {
            _settleFrontendPayouts(_targetRank, newNodeId, _recipients, _amounts, _vaultTargetNodeId, _vaultAmount, targetPrice);
        }
    }

    function upgradeRankForMainId(uint256 _r1MainId, uint8 _targetRank) public nonReentrant {
        _collectUpgradePayment(_r1MainId, _targetRank);
        _applyRankUpgrade(_r1MainId, msg.sender, _targetRank, false, 0, true, true);
    }

    function upgradeRank(uint8 _targetRank) external {
        uint256 r1MainId = users[msg.sender].firstNodeId[1];
        if (r1MainId == 0) revert ErrNotRegistered();
        upgradeRankForMainId(r1MainId, _targetRank);
    }

    function adminUpgradeRankForMainId(uint256 _r1MainId, uint8 _targetRank) public onlyOwner nonReentrant {
        if (_r1MainId == 0 || _r1MainId > totalNodesInRank[1]) revert ErrBadParam();
        MatrixNode storage mNode = rankNodes[1][_r1MainId];
        if (mNode.owner == address(0) || mNode.isClone || mNode.isExtraId) revert ErrBadParam();
        if (_targetRank <= 1 || _targetRank > MAX_RANK) revert ErrInvalidRank();

        address targetUser = mNode.owner;
        uint8 currentRank = getMainIdCurrentRank(_r1MainId);
        if (_targetRank <= currentRank) revert ErrRankActive();

        for (uint8 r = currentRank + 1; r <= _targetRank;) {
            _applyRankUpgrade(_r1MainId, targetUser, r, false, 0, true, true);
            unchecked { ++r; }
        }
    }

    function adminUpgradeRank(address _user, uint8 _targetRank) external onlyOwner {
        if (!users[_user].isRegistered) revert ErrNotRegistered();
        uint256 r1MainId = users[_user].firstNodeId[1];
        if (r1MainId == 0) revert ErrBadParam();
        adminUpgradeRankForMainId(r1MainId, _targetRank);
    }

    function _applyRankUpgrade(
        uint256 _r1MainId,
        address _user,
        uint8 _targetRank,
        bool _isAuto,
        uint256 _overrideParentId,
        bool _overrideIsLeft,
        bool _payOnChainRewards
    ) internal returns (uint256 newNodeId) {
        if (mainIdCurrentMaxRank[_r1MainId] < _targetRank) {
            mainIdCurrentMaxRank[_r1MainId] = _targetRank;
        }
        if (users[_user].currentMaxRank < _targetRank) {
            users[_user].currentMaxRank = _targetRank;
        }

        uint256 r1SponsorNodeId = nodeDirectSponsorNodeId[1][_r1MainId];
        if (r1SponsorNodeId == 0) r1SponsorNodeId = users[_user].sponsorMainNodeId;
        if (r1SponsorNodeId == 0) r1SponsorNodeId = 1;
        address sponsor = rankNodes[1][r1SponsorNodeId].owner;
        if (sponsor == address(0)) sponsor = rankNodes[_targetRank][1].owner;

        uint256 sponsorInTarget = getMainIdNodeInRank(r1SponsorNodeId, _targetRank);
        newNodeId = _insertNode(_targetRank, _user, sponsor, sponsorInTarget, _overrideParentId, _overrideIsLeft, false, false, _payOnChainRewards);

        nodeOriginMainId[_targetRank][newNodeId] = _r1MainId;
        nodeDirectSponsorNodeId[_targetRank][newNodeId] = r1SponsorNodeId;
        mainIdNodeInRank[_r1MainId][_targetRank] = newNodeId;
        nodeToR1MainId[_targetRank][newNodeId] = _r1MainId;

        emit RankUpgraded(_user, _targetRank - 1, _targetRank, newNodeId, _isAuto);
    }

    function _insertNode(
        uint8 _rank,
        address _owner,
        address _sponsor,
        uint256 _sponsorNodeId,
        uint256 _targetParentId,
        bool _isLeft,
        bool _isClone,
        bool _isExtraId,
        bool _payOnChainRewards
    ) internal returns (uint256 newNodeId) {
        return _insertNodeWithOrigin(
            _rank,
            _owner,
            _sponsor,
            _sponsorNodeId,
            _targetParentId,
            _isLeft,
            _isClone,
            _isExtraId,
            0,
            0,
            _payOnChainRewards
        );
    }

    function _insertNodeWithOrigin(
        uint8 _rank,
        address _owner,
        address _sponsor,
        uint256 _sponsorNodeId,
        uint256 _targetParentId,
        bool _isLeft,
        bool _isClone,
        bool _isExtraId,
        uint256 _cloneOriginId,
        uint8 _cloneRoundVal,
        bool _payOnChainRewards
    ) internal returns (uint256 newNodeId) {
        uint256 pId = _targetParentId;
        bool isL = _isLeft;
        if (
            pId == 0 ||
            pId > totalNodesInRank[_rank] ||
            (isL && rankNodes[_rank][pId].leftChild != 0) ||
            (!isL && rankNodes[_rank][pId].rightChild != 0)
        ) {
            (pId, isL) = _findPlacementSpot(_rank, _sponsor, _sponsorNodeId);
        }

        newNodeId = ++totalNodesInRank[_rank];
        MatrixNode storage n = rankNodes[_rank][newNodeId];
        n.id = newNodeId;
        n.owner = _owner;
        n.directSponsor = _sponsor;
        n.parentId = pId;
        n.isClone = _isClone;
        n.isExtraId = _isExtraId;

        if (isL) {
            rankNodes[_rank][pId].leftChild = newNodeId;
        } else {
            rankNodes[_rank][pId].rightChild = newNodeId;
        }

        if (users[_owner].firstNodeId[_rank] == 0) {
            users[_owner].firstNodeId[_rank] = newNodeId;
        }
        users[_owner].latestNodeId[_rank] = newNodeId;

        if (!_isClone) {
            nodeOriginMainId[_rank][newNodeId] = newNodeId;
            nodeDirectSponsorNodeId[_rank][newNodeId] = _sponsorNodeId > 0 ? _sponsorNodeId : 1;
            if (_rank == 1 && !_isExtraId) {
                mainIdCurrentMaxRank[newNodeId] = 1;
                mainIdNodeInRank[newNodeId][1] = newNodeId;
                nodeToR1MainId[1][newNodeId] = newNodeId;
            }
        } else {
            uint256 resolvedOrigin = _cloneOriginId != 0 ? _cloneOriginId : newNodeId;
            nodeOriginMainId[_rank][newNodeId] = resolvedOrigin;
            nodeDirectSponsorNodeId[_rank][newNodeId] = _sponsorNodeId > 0 ? _sponsorNodeId : 1;
            mainNodeLatestCloneId[_rank][resolvedOrigin] = newNodeId;
            nodeCloneRound[_rank][newNodeId] = _cloneRoundVal > 0 ? _cloneRoundVal : 1;
            nodeCloneTotalCount[_rank][resolvedOrigin] += 1;
        }

        emit NodeCreated(_rank, newNodeId, _owner, _sponsor, pId, isL, _isClone, _isExtraId);

        if (isL) {
            if (_payOnChainRewards) _distributeLeftChildRewards(_rank, newNodeId, pId);
        } else {
            _triggerRightChildClone(_rank, pId, _payOnChainRewards);
        }
    }

    function _spawnCloneNode(
        uint8 _rank,
        address _owner,
        address _sponsor,
        uint256 _cloneOriginMainId,
        uint256 _targetSponsorNodeId,
        bool _payRewards
    ) internal returns (uint256 newNodeId) {
        uint256 originId = _cloneOriginMainId != 0 ? _cloneOriginMainId : 1;
        uint8 currentRound = uint8(nodeCloneTotalCount[_rank][originId] % 2);
        sponsorRoundToggle[_rank][originId] = currentRound == 0 ? 1 : 0;

        uint256 sponsorInRankId = _targetSponsorNodeId;
        if (_rank > 1 && sponsorInRankId > 1) {
            uint256 mappedInR = getMainIdNodeInRank(sponsorInRankId, _rank);
            sponsorInRankId = mappedInR;
        }

        (uint256 pId, bool isL) = (currentRound == 0 && sponsorInRankId > 0)
            ? _findPlacementSpot(_rank, _sponsor, sponsorInRankId)
            : _findFirstAvailableUnder(_rank, 1);

        newNodeId = _insertNodeWithOrigin(
            _rank,
            _owner,
            _sponsor,
            _targetSponsorNodeId,
            pId,
            isL,
            true,
            false,
            originId,
            currentRound == 0 ? 1 : 2,
            _payRewards
        );
    }

    function _triggerRightChildClone(uint8 _rank, uint256 _parentId, bool _payRewards) internal {
        MatrixNode storage pNode = rankNodes[_rank][_parentId];
        address parentOwner = pNode.owner;
        address parentSponsor = pNode.directSponsor != address(0) ? pNode.directSponsor : parentOwner;
        uint256 originId = nodeOriginMainId[_rank][_parentId];
        if (originId == 0) originId = _parentId;
        uint256 spNodeId = nodeDirectSponsorNodeId[_rank][_parentId];
        if (spNodeId == 0) spNodeId = 1;

        if (_payRewards && gasleft() < 350000) {
            pendingCloneQueue.push(PendingCloneItem({
                rank: _rank,
                parentOwner: parentOwner,
                parentSponsor: parentSponsor,
                cloneOriginMainId: originId,
                targetSponsorNodeId: spNodeId,
                r1SponsorId: spNodeId
            }));
            emit CloneQueued(_rank, originId, spNodeId);
            return;
        }

        _spawnCloneNode(_rank, parentOwner, parentSponsor, originId, spNodeId, _payRewards);
    }

    function _findPlacementSpot(uint8 _rank, address _sponsor, uint256 _sponsorNodeId) internal view returns (uint256, bool) {
        uint256 primaryId = _sponsorNodeId > 0 ? _sponsorNodeId : users[_sponsor].firstNodeId[_rank];
        if (primaryId == 0 || primaryId > totalNodesInRank[_rank]) {
            return _findFirstAvailableUnder(_rank, 1);
        }

        // Priority 1: ติดตัวผู้แนะนำตรง (Direct Sponsor) ซ้ายก่อน ขวาตาม
        MatrixNode storage primaryNode = rankNodes[_rank][primaryId];
        if (primaryNode.leftChild == 0) return (primaryId, true);
        if (primaryNode.rightChild == 0) return (primaryId, false);

        // Priority 2: หากผู้แนะนำตรงเต็ม ให้วิ่งไปต่อไอดีโคลนนิ่งของผู้แนะนำตรง (ซ้ายก่อน ขวาตาม เรียงตามลำดับโหนด)
        uint256 originTag = nodeOriginMainId[_rank][primaryId];
        if (originTag == 0) originTag = primaryId;
        uint256 total = totalNodesInRank[_rank];
        for (uint256 cid = primaryId + 1; cid <= total; cid++) {
            MatrixNode storage cand = rankNodes[_rank][cid];
            if (!cand.isClone) continue;
            uint256 candOrig = nodeOriginMainId[_rank][cid];
            if (candOrig == primaryId || candOrig == originTag) {
                if (cand.leftChild == 0) return (cid, true);
                if (cand.rightChild == 0) return (cid, false);
            }
        }

        // Priority 3: Fallback BFS ภายใต้สายงานผู้แนะนำตรง
        return _findFirstAvailableUnder(_rank, primaryId);
    }

    function _findFirstAvailableUnder(uint8 _rank, uint256 _startNodeId) internal view returns (uint256, bool) {
        if (_startNodeId == 0) _startNodeId = 1;
        uint256[32] memory queue;
        uint256 head = 0;
        uint256 tail = 0;
        queue[tail++] = _startNodeId;

        while (head < tail) {
            uint256 currentId = queue[head++];
            MatrixNode memory node = rankNodes[_rank][currentId];

            if (node.leftChild == 0) return (currentId, true);
            if (node.rightChild == 0) return (currentId, false);

            if (tail < 30) {
                queue[tail++] = node.leftChild;
                queue[tail++] = node.rightChild;
            }
        }
        return (_startNodeId, true);
    }

    function _payAndEmit(
        uint256 _fromId,
        uint256 _toId,
        address _toAddr,
        uint8 _rank,
        string memory _rewardType,
        uint256 _amount
    ) internal {
        if (_amount > 0 && _toAddr != address(0)) {
            usdt.safeTransfer(_toAddr, _amount);
            emit RewardDistributed(_fromId, _toId, _toAddr, _rank, _rewardType, _amount);
        }
    }

    function _distributeLeftChildRewards(uint8 _rank, uint256 _fromNodeId, uint256 _parentId) internal {
        uint256 totalAmount = rankPrice[_rank];
        uint256 directReward = (totalAmount * 30) / 100;

        uint256 sponsorNodeId = nodeDirectSponsorNodeId[_rank][_fromNodeId];
        address sponsor = rankNodes[_rank][_fromNodeId].directSponsor;
        if (sponsor == address(0) && sponsorNodeId > 0) {
            sponsor = rankNodes[1][sponsorNodeId].owner;
        }
        bool isId1Clone = rankNodes[_rank][_fromNodeId].isClone && nodeOriginMainId[_rank][_fromNodeId] == 1;

        if (isId1Clone || sponsor == address(0)) {
            _payAndEmit(_fromNodeId, 0, familyPool, _rank, "DIRECT_ID1_CLONE_FAMILY_POOL", directReward);
        } else {
            _payAndEmit(_fromNodeId, sponsorNodeId, sponsor, _rank, "DIRECT", directReward);
        }

        uint256 levelRewardPerTier = (totalAmount * 2) / 100;
        uint256 currentUplineId = _parentId;
        uint256 unpaidLevelAmount = 0;

        for (uint8 i = 1; i <= MAX_LEVELS;) {
            if (currentUplineId != 0) {
                MatrixNode memory upline = rankNodes[_rank][currentUplineId];
                _payAndEmit(_fromNodeId, currentUplineId, upline.owner, _rank, "LEVEL", levelRewardPerTier);
                currentUplineId = upline.parentId;
            } else {
                unpaidLevelAmount += levelRewardPerTier;
            }
            unchecked { ++i; }
        }

        uint256 upgradeAmount = (totalAmount * 35) / 100;
        uint256 parentOriginId = nodeOriginMainId[_rank][_parentId];
        if (parentOriginId == 0) parentOriginId = _parentId;
        _creditVault(_rank, _fromNodeId, parentOriginId, upgradeAmount);

        _payAndEmit(_fromNodeId, 0, familyPool, _rank, "UNPAID_LEVEL_FAMILY_POOL", unpaidLevelAmount);
        _payAndEmit(_fromNodeId, 0, allSellWallet, _rank, "ALLSELL", (totalAmount * 4) / 100);
        _payAndEmit(_fromNodeId, 0, botWallet, _rank, "BOT", (totalAmount * 1) / 100);
    }

    function emergencyWithdrawToken(address _token, uint256 _amount) external onlyOwner nonReentrant {
        if (_token == address(0)) revert ErrZero();
        IERC20 token = IERC20(_token);
        require(token.balanceOf(address(this)) >= _amount, "Low balance");
        token.safeTransfer(msg.sender, _amount);
    }

    function emergencyWithdrawNative() external onlyOwner {
        uint256 balance = address(this).balance;
        if (balance == 0) revert ErrZero();
        (bool success, ) = payable(msg.sender).call{value: balance}("");
        require(success, "Failed");
    }

    function setSystemWallets(address _allSell, address _bot, address _familyPool) external onlyOwner {
        if (_allSell == address(0) || _bot == address(0) || _familyPool == address(0)) revert ErrZero();
        allSellWallet = _allSell;
        botWallet = _bot;
        familyPool = _familyPool;
    }

    function getUserProfile(address _user)
        external
        view
        returns (
            bool isRegistered,
            address sponsor,
            uint256 sponsorMainNodeId,
            uint8 currentMaxRank,
            uint256 mainNodeIdRank1,
            uint256 latestCloneIdRank1
        )
    {
        return (
            users[_user].isRegistered,
            users[_user].sponsor,
            users[_user].sponsorMainNodeId,
            users[_user].currentMaxRank,
            users[_user].firstNodeId[1],
            users[_user].latestNodeId[1]
        );
    }

    function getUserRankInfo(address _user, uint8 _rank)
        external
        view
        returns (
            uint256 firstNodeId,
            uint256 latestNodeId,
            uint256 upgradeVaultBalance,
            uint256 nextRankPrice
        )
    {
        uint256 nextPrice = _rank < MAX_RANK ? rankPrice[_rank + 1] : 0;
        return (
            users[_user].firstNodeId[_rank],
            users[_user].latestNodeId[_rank],
            users[_user].upgradeVault[_rank],
            nextPrice
        );
    }

    function getNodeCloneRound(uint8 _rank, uint256 _nodeId) external view returns (uint8) {
        return nodeCloneRound[_rank][_nodeId];
    }

    function getNodeCloneTotalCount(uint8 _rank, uint256 _originMainId) external view returns (uint256) {
        return nodeCloneTotalCount[_rank][_originMainId];
    }
}
