package org.qora.transaction;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.qora.account.Account;
import org.qora.account.PublicKeyAccount;
import org.qora.asset.Asset;
import org.qora.crypto.Crypto;
import org.qora.data.transaction.UpdateGroupTransactionData;
import org.qora.data.group.GroupData;
import org.qora.data.transaction.TransactionData;
import org.qora.group.Group;
import org.qora.repository.DataException;
import org.qora.repository.Repository;

import com.google.common.base.Utf8;

public class UpdateGroupTransaction extends Transaction {

	// Properties
	private UpdateGroupTransactionData updateGroupTransactionData;

	// Constructors

	public UpdateGroupTransaction(Repository repository, TransactionData transactionData) {
		super(repository, transactionData);

		this.updateGroupTransactionData = (UpdateGroupTransactionData) this.transactionData;
	}

	// More information

	@Override
	public List<Account> getRecipientAccounts() throws DataException {
		return Collections.singletonList(getNewOwner());
	}

	@Override
	public boolean isInvolved(Account account) throws DataException {
		String address = account.getAddress();

		if (address.equals(this.getOwner().getAddress()))
			return true;

		if (address.equals(this.getNewOwner().getAddress()))
			return true;

		return false;
	}

	@Override
	public BigDecimal getAmount(Account account) throws DataException {
		String address = account.getAddress();
		BigDecimal amount = BigDecimal.ZERO.setScale(8);

		if (address.equals(this.getOwner().getAddress()))
			amount = amount.subtract(this.transactionData.getFee());

		return amount;
	}

	// Navigation

	public Account getOwner() throws DataException {
		return new PublicKeyAccount(this.repository, this.updateGroupTransactionData.getOwnerPublicKey());
	}

	public Account getNewOwner() throws DataException {
		return new Account(this.repository, this.updateGroupTransactionData.getNewOwner());
	}

	// Processing

	@Override
	public ValidationResult isValid() throws DataException {
		// Check new owner address is valid
		if (!Crypto.isValidAddress(updateGroupTransactionData.getNewOwner()))
			return ValidationResult.INVALID_ADDRESS;

		// Check group name size bounds
		int groupNameLength = Utf8.encodedLength(updateGroupTransactionData.getGroupName());
		if (groupNameLength < 1 || groupNameLength > Group.MAX_NAME_SIZE)
			return ValidationResult.INVALID_NAME_LENGTH;

		// Check new description size bounds
		int newDescriptionLength = Utf8.encodedLength(updateGroupTransactionData.getNewDescription());
		if (newDescriptionLength < 1 || newDescriptionLength > Group.MAX_DESCRIPTION_SIZE)
			return ValidationResult.INVALID_DESCRIPTION_LENGTH;

		// Check group name is lowercase
		if (!updateGroupTransactionData.getGroupName().equals(updateGroupTransactionData.getGroupName().toLowerCase()))
			return ValidationResult.NAME_NOT_LOWER_CASE;

		GroupData groupData = this.repository.getGroupRepository().fromGroupName(updateGroupTransactionData.getGroupName());

		// Check group exists
		if (groupData == null)
			return ValidationResult.GROUP_DOES_NOT_EXIST;

		// Check transaction's public key matches group's current owner
		Account owner = getOwner();
		if (!owner.getAddress().equals(groupData.getOwner()))
			return ValidationResult.INVALID_GROUP_OWNER;

		// Check fee is positive
		if (updateGroupTransactionData.getFee().compareTo(BigDecimal.ZERO) <= 0)
			return ValidationResult.NEGATIVE_FEE;

		// Check reference is correct
		Account creator = getCreator();

		if (!Arrays.equals(creator.getLastReference(), updateGroupTransactionData.getReference()))
			return ValidationResult.INVALID_REFERENCE;

		// Check creator has enough funds
		if (creator.getConfirmedBalance(Asset.QORA).compareTo(updateGroupTransactionData.getFee()) < 0)
			return ValidationResult.NO_BALANCE;

		return ValidationResult.OK;
	}

	@Override
	public void process() throws DataException {
		// Update Group
		Group group = new Group(this.repository, updateGroupTransactionData.getGroupName());
		group.update(updateGroupTransactionData);

		// Save this transaction, now with updated "group reference" to previous transaction that updated group
		this.repository.getTransactionRepository().save(updateGroupTransactionData);

		// Update owner's balance
		Account owner = getOwner();
		owner.setConfirmedBalance(Asset.QORA, owner.getConfirmedBalance(Asset.QORA).subtract(updateGroupTransactionData.getFee()));

		// Update owner's reference
		owner.setLastReference(updateGroupTransactionData.getSignature());
	}

	@Override
	public void orphan() throws DataException {
		// Revert name
		Group group = new Group(this.repository, updateGroupTransactionData.getGroupName());
		group.revert(updateGroupTransactionData);

		// Delete this transaction itself
		this.repository.getTransactionRepository().delete(updateGroupTransactionData);

		// Update owner's balance
		Account owner = getOwner();
		owner.setConfirmedBalance(Asset.QORA, owner.getConfirmedBalance(Asset.QORA).add(updateGroupTransactionData.getFee()));

		// Update owner's reference
		owner.setLastReference(updateGroupTransactionData.getReference());
	}

}