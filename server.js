require('dotenv').config();
const express = require('express');
const { Sequelize, DataTypes, Op } = require('sequelize');
const dayjs = require('dayjs');

// ---------------------- CONFIG ---------------------- //

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

const sequelize = new Sequelize(
  process.env.DB_NAME || 'library_db',
  process.env.DB_USER || 'USER',
  process.env.DB_PASS || 'Jaipubg@123',
  {
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 5432,
    dialect: 'postgres',
    logging: false,
  }
);

const LOAN_DAYS = 14;
const FINE_PER_DAY = 0.5;
const Book = sequelize.define('Book', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  isbn: { type: DataTypes.STRING, allowNull: false, unique: true },
  title: { type: DataTypes.STRING, allowNull: false },
  author: { type: DataTypes.STRING, allowNull: false },
  category: { type: DataTypes.STRING },
  status: {
    type: DataTypes.ENUM('available', 'borrowed', 'reserved', 'maintenance'),
    defaultValue: 'available',
    allowNull: false,
  },
  total_copies: {
    type: DataTypes.INTEGER,
    allowNull: false,
    defaultValue: 1,
    validate: { min: 0 },
  },
  available_copies: {
    type: DataTypes.INTEGER,
    allowNull: false,
    defaultValue: 1,
    validate: { min: 0 },
  },
}, {
  tableName: 'books',
  underscored: true,
});

const Member = sequelize.define('Member', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  name: { type: DataTypes.STRING, allowNull: false },
  email: { type: DataTypes.STRING, unique: true, allowNull: false },
  membership_number: { type: DataTypes.STRING, unique: true, allowNull: false },
  status: {
    type: DataTypes.ENUM('active', 'suspended'),
    defaultValue: 'active',
    allowNull: false,
  },
}, {
  tableName: 'members',
  underscored: true,
});

const Transaction = sequelize.define('Transaction', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  borrowed_at: { type: DataTypes.DATE, allowNull: false },
  due_date: { type: DataTypes.DATE, allowNull: false },
  returned_at: { type: DataTypes.DATE, allowNull: true },
  status: {
    type: DataTypes.ENUM('active', 'returned', 'overdue'),
    defaultValue: 'active',
    allowNull: false,
  },
}, {
  tableName: 'transactions',
  underscored: true,
});

const Fine = sequelize.define('Fine', {
  id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  amount: { type: DataTypes.DECIMAL(10, 2), allowNull: false },
  paid_at: { type: DataTypes.DATE, allowNull: true },
}, {
  tableName: 'fines',
  underscored: true,
});

Book.hasMany(Transaction, { foreignKey: { name: 'book_id', allowNull: false } });
Transaction.belongsTo(Book, { foreignKey: 'book_id' });

Member.hasMany(Transaction, { foreignKey: { name: 'member_id', allowNull: false } });
Transaction.belongsTo(Member, { foreignKey: 'member_id' });

Member.hasMany(Fine, { foreignKey: { name: 'member_id', allowNull: false } });
Fine.belongsTo(Member, { foreignKey: 'member_id' });

Transaction.hasMany(Fine, { foreignKey: { name: 'transaction_id', allowNull: false } });
Fine.belongsTo(Transaction, { foreignKey: 'transaction_id' });

async function updateGlobalOverdues() {
  const now = new Date();
  const [count] = await Transaction.update(
    { status: 'overdue' },
    { where: { status: 'active', due_date: { [Op.lt]: now } } }
  );
  if (count > 0) {
    // For any member affected, recompute status
    const overdueTxs = await Transaction.findAll({
      where: { status: 'overdue', due_date: { [Op.lt]: now } },
      attributes: ['member_id'],
      group: ['member_id'],
    });
    for (const row of overdueTxs) {
      await recomputeMemberStatus(row.member_id);
    }
  }
}

// recompute status for a member based on concurrent overdue count
async function recomputeMemberStatus(memberId) {
  const overdueCount = await Transaction.count({
    where: { member_id: memberId, status: 'overdue' },
  });

  const member = await Member.findByPk(memberId);
  if (!member) return;

  if (overdueCount >= 3) {
    member.status = 'suspended';
  } else if (member.status === 'suspended') {
    // bring them back if suspension was only due to overdue count
    member.status = 'active';
  }
  await member.save();
}

// ---------------------- BUSINESS LOGIC ---------------------- //

// Borrow a book
async function borrowBook(memberId, bookId) {
  await updateGlobalOverdues();

  return sequelize.transaction(async (t) => {
    const member = await Member.findByPk(memberId, { transaction: t, lock: t.LOCK.UPDATE });
    if (!member) {
      const err = new Error('Member not found');
      err.status = 404;
      throw err;
    }
    if (member.status !== 'active') {
      const err = new Error('Member is not active (possibly suspended)');
      err.status = 403;
      throw err;
    }

    const unpaidFines = await Fine.count({
      where: { member_id: memberId, paid_at: null },
      transaction: t,
    });
    if (unpaidFines > 0) {
      const err = new Error('Member has unpaid fines');
      err.status = 403;
      throw err;
    }

    const activeBorrows = await Transaction.count({
      where: {
        member_id: memberId,
        status: { [Op.in]: ['active', 'overdue'] }, // still borrowed
      },
      transaction: t,
    });
    if (activeBorrows >= 3) {
      const err = new Error('Borrow limit reached (3 books)');
      err.status = 403;
      throw err;
    }

    const book = await Book.findByPk(bookId, { transaction: t, lock: t.LOCK.UPDATE });
    if (!book) {
      const err = new Error('Book not found');
      err.status = 404;
      throw err;
    }

    if (book.status !== 'available' || book.available_copies <= 0) {
      const err = new Error('Book is not available to borrow');
      err.status = 400;
      throw err;
    }

    const now = new Date();
    const due = dayjs(now).add(LOAN_DAYS, 'day').toDate();

    const tx = await Transaction.create({
      member_id: memberId,
      book_id: bookId,
      borrowed_at: now,
      due_date: due,
      status: 'active',
    }, { transaction: t });

    book.available_copies -= 1;
    if (book.available_copies <= 0) {
      book.available_copies = 0;
      book.status = 'borrowed';
    }
    await book.save({ transaction: t });

    return tx;
  });
}

// Return a book
async function returnBook(transactionId) {
  await updateGlobalOverdues();

  return sequelize.transaction(async (t) => {
    const tx = await Transaction.findByPk(transactionId, {
      transaction: t,
      lock: t.LOCK.UPDATE,
    });
    if (!tx) {
      const err = new Error('Transaction not found');
      err.status = 404;
      throw err;
    }

    if (tx.status === 'returned') {
      const err = new Error('Book already returned');
      err.status = 400;
      throw err;
    }

    const now = new Date();
    tx.returned_at = now;
    tx.status = 'returned';
    await tx.save({ transaction: t });

    // compute overdue days (ceil)
    let fineAmount = 0;
    if (now > tx.due_date) {
      const msDiff = now.getTime() - tx.due_date.getTime();
      const days = Math.ceil(msDiff / (1000 * 60 * 60 * 24));
      fineAmount = days * FINE_PER_DAY;
      if (fineAmount > 0) {
        await Fine.create({
          member_id: tx.member_id,
          transaction_id: tx.id,
          amount: fineAmount.toFixed(2),
        }, { transaction: t });
      }
    }

    // update book
    const book = await Book.findByPk(tx.book_id, {
      transaction: t,
      lock: t.LOCK.UPDATE,
    });
    book.available_copies = Math.min(book.total_copies, book.available_copies + 1);
    if (book.status !== 'maintenance' && book.status !== 'reserved') {
      book.status = 'available';
    }
    await book.save({ transaction: t });

    // recompute member status (overdue count may drop)
    await recomputeMemberStatus(tx.member_id);

    return { transaction: tx, fineAmount };
  });
}

// Mark fine as paid
async function payFine(fineId) {
  const fine = await Fine.findByPk(fineId);
  if (!fine) {
    const err = new Error('Fine not found');
    err.status = 404;
    throw err;
  }
  if (fine.paid_at) {
    const err = new Error('Fine is already paid');
    err.status = 400;
    throw err;
  }
  fine.paid_at = new Date();
  await fine.save();
  return fine;
}

// ---------------------- ROUTES ---------------------- //

// Healthcheck
app.get('/', (req, res) => {
  res.json({ message: 'Library API running' });
});

// ---------- Books CRUD ---------- //

// Create book
app.post('/books', async (req, res, next) => {
  try {
    const { isbn, title, author, category, total_copies } = req.body;
    const copies = total_copies ?? 1;
    const book = await Book.create({
      isbn, title, author, category,
      total_copies: copies,
      available_copies: copies,
      status: 'available',
    });
    res.status(201).json(book);
  } catch (err) { next(err); }
});

// List all books
app.get('/books', async (req, res, next) => {
  try {
    const books = await Book.findAll();
    res.json(books);
  } catch (err) { next(err); }
});

// List available books
app.get('/books/available', async (req, res, next) => {
  try {
    const books = await Book.findAll({
      where: { status: 'available', available_copies: { [Op.gt]: 0 } },
    });
    res.json(books);
  } catch (err) { next(err); }
});

// Get book by id
app.get('/books/:id', async (req, res, next) => {
  try {
    const book = await Book.findByPk(req.params.id);
    if (!book) return res.status(404).json({ error: 'Not found' });
    res.json(book);
  } catch (err) { next(err); }
});

// Update book
app.put('/books/:id', async (req, res, next) => {
  try {
    const book = await Book.findByPk(req.params.id);
    if (!book) return res.status(404).json({ error: 'Not found' });
    const { isbn, title, author, category, status, total_copies, available_copies } = req.body;
    if (isbn !== undefined) book.isbn = isbn;
    if (title !== undefined) book.title = title;
    if (author !== undefined) book.author = author;
    if (category !== undefined) book.category = category;
    if (status !== undefined) book.status = status;
    if (total_copies !== undefined) book.total_copies = total_copies;
    if (available_copies !== undefined) book.available_copies = available_copies;
    await book.save();
    res.json(book);
  } catch (err) { next(err); }
});

// Delete book
app.delete('/books/:id', async (req, res, next) => {
  try {
    const count = await Book.destroy({ where: { id: req.params.id } });
    if (!count) return res.status(404).json({ error: 'Not found' });
    res.status(204).send();
  } catch (err) { next(err); }
});

// ---------- Members CRUD ---------- //

// Create member
app.post('/members', async (req, res, next) => {
  try {
    const { name, email, membership_number } = req.body;
    const member = await Member.create({ name, email, membership_number, status: 'active' });
    res.status(201).json(member);
  } catch (err) { next(err); }
});

// List members
app.get('/members', async (req, res, next) => {
  try {
    const members = await Member.findAll();
    res.json(members);
  } catch (err) { next(err); }
});

// Get member
app.get('/members/:id', async (req, res, next) => {
  try {
    const member = await Member.findByPk(req.params.id);
    if (!member) return res.status(404).json({ error: 'Not found' });
    res.json(member);
  } catch (err) { next(err); }
});

// Update member
app.put('/members/:id', async (req, res, next) => {
  try {
    const member = await Member.findByPk(req.params.id);
    if (!member) return res.status(404).json({ error: 'Not found' });
    const { name, email, membership_number, status } = req.body;
    if (name !== undefined) member.name = name;
    if (email !== undefined) member.email = email;
    if (membership_number !== undefined) member.membership_number = membership_number;
    if (status !== undefined) member.status = status;
    await member.save();
    res.json(member);
  } catch (err) { next(err); }
});

// Delete member
app.delete('/members/:id', async (req, res, next) => {
  try {
    const count = await Member.destroy({ where: { id: req.params.id } });
    if (!count) return res.status(404).json({ error: 'Not found' });
    res.status(204).send();
  } catch (err) { next(err); }
});

// Books currently borrowed by a member
app.get('/members/:id/borrowed', async (req, res, next) => {
  try {
    await updateGlobalOverdues();
    const memberId = req.params.id;
    const txs = await Transaction.findAll({
      where: {
        member_id: memberId,
        status: { [Op.in]: ['active', 'overdue'] },
      },
      include: [Book],
    });
    res.json(txs);
  } catch (err) { next(err); }
});

// ---------- Transactions ---------- //

// Borrow a book
app.post('/transactions/borrow', async (req, res, next) => {
  try {
    const { member_id, book_id } = req.body;
    if (!member_id || !book_id) {
      return res.status(400).json({ error: 'member_id and book_id are required' });
    }
    const tx = await borrowBook(member_id, book_id);
    res.status(201).json(tx);
  } catch (err) { next(err); }
});

// Return a book
app.post('/transactions/:id/return', async (req, res, next) => {
  try {
    const result = await returnBook(req.params.id);
    res.json(result);
  } catch (err) { next(err); }
});

// List overdue transactions
app.get('/transactions/overdue', async (req, res, next) => {
  try {
    await updateGlobalOverdues();
    const overdue = await Transaction.findAll({
      where: { status: 'overdue' },
      include: [Book, Member],
    });
    res.json(overdue);
  } catch (err) { next(err); }
});

// ---------- Fines ---------- //

// List fines (optional)
app.get('/fines', async (req, res, next) => {
  try {
    const fines = await Fine.findAll({ include: [Member, Transaction] });
    res.json(fines);
  } catch (err) { next(err); }
});

// Pay fine
app.post('/fines/:id/pay', async (req, res, next) => {
  try {
    const fine = await payFine(req.params.id);
    res.json(fine);
  } catch (err) { next(err); }
});

// ---------------------- ERROR HANDLER ---------------------- //

app.use((err, req, res, next) => { // eslint-disable-line
  console.error(err);
  const status = err.status || 500;
  res.status(status).json({
    error: err.message || 'Internal Server Error',
  });
});

// ---------------------- STARTUP ---------------------- //

(async () => {
  try {
    await sequelize.authenticate();
    console.log('DB connection OK');
    await sequelize.sync(); // for production, use migrations instead
    console.log('DB synced');
    app.listen(PORT, () => {
      console.log(`Library API running on http://localhost:${PORT}`);
    });
  } catch (err) {
    console.error('Failed to start app:', err);
    process.exit(1);
  }
})();
