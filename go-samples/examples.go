/*
Copyright 2020 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
  "context"
  "errors"
  "fmt"
  "log"
  "os"
  "sync"
  "time"

  "cloud.google.com/go/spanner"
  "google.golang.org/api/iterator"
  sppb "google.golang.org/genproto/googleapis/spanner/v1"
  "google.golang.org/grpc"
  "google.golang.org/grpc/codes"
  "google.golang.org/grpc/status"
  "google.golang.org/api/option"
)

// Options encapsulates options for the emulator wrapper.
type EmulatorOptions struct {
  // Address at which emulator process is running (e.g., localhost:9010).
  hostport string

  // project/instance/database URI
  uri string
}

// ClientOptions needed by go client library to talk to emulator.
func (emu *EmulatorOptions) ClientOptions() []option.ClientOption {
  return []option.ClientOption{
    option.WithEndpoint(emu.hostport),
    option.WithoutAuthentication(),
    option.WithGRPCDialOption(grpc.WithInsecure()),
  }
}

func ExampleNewClient(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  _ = client // TODO: Use client.
}

func ExampleClient_Single(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  iter := client.Single().Query(ctx, spanner.NewStatement("SELECT FirstName FROM Singers"))
  _ = iter // TODO: iterate using Next or Do.
}

func ExampleClient_ReadOnlyTransaction(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  t := client.ReadOnlyTransaction()
  defer t.Close()
  // TODO: Read with t using Read, ReadRow, ReadUsingIndex, or Query.
}

func ExampleClient_ReadWriteTransaction(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  _, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
    var balance int64
    row, err := txn.ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"balance"})
    if err != nil {
      // This function will be called again if this is an IsAborted error.
      return err
    }
    if err := row.Column(0, &balance); err != nil {
      return err
    }

    if balance <= 10 {
      return errors.New("insufficient funds in account")
    }
    balance -= 10
    m := spanner.Update("Accounts", []string{"user", "balance"}, []interface{}{"alice", balance})
    return txn.BufferWrite([]*spanner.Mutation{m})
    // The buffered mutation will be committed. If the commit fails with an
    // IsAborted error, this function will be called again.
  })
  if err != nil {
    log.Fatal(err)
  }
}

func ExampleUpdate(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  _, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
    row, err := txn.ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"balance"})
    if err != nil {
      return err
    }
    var balance int64
    if err := row.Column(0, &balance); err != nil {
      return err
    }
    return txn.BufferWrite([]*spanner.Mutation{
      spanner.Update("Accounts", []string{"user", "balance"}, []interface{}{"alice", balance + 10}),
    })
  })
  if err != nil {
    log.Fatal(err)
  }
}

// This example is the same as the one for Update, except for the use of UpdateMap.
func ExampleUpdateMap(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  _, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
    row, err := txn.ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"balance"})
    if err != nil {
      return err
    }
    var balance int64
    if err := row.Column(0, &balance); err != nil {
      return err
    }
    return txn.BufferWrite([]*spanner.Mutation{
      spanner.UpdateMap("Accounts", map[string]interface{}{
        "user":    "alice",
        "balance": balance + 10,
      }),
    })
  })
  if err != nil {
    log.Fatal(err)
  }
}

// This example is the same as the one for Update, except for the use of
// UpdateStruct.
func ExampleUpdateStruct(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  type account struct {
    User    string `spanner:"user"`
    Balance int64  `spanner:"balance"`
  }
  _, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
    row, err := txn.ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"balance"})
    if err != nil {
      return err
    }
    var balance int64
    if err := row.Column(0, &balance); err != nil {
      return err
    }
    m, err := spanner.UpdateStruct("Accounts", account{
      User:    "alice",
      Balance: balance + 10,
    })
    if err != nil {
      return err
    }
    return txn.BufferWrite([]*spanner.Mutation{m})
  })
  if err != nil {
    log.Fatal(err)
  }
}

func ExampleClient_Apply(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  m := spanner.Update("Users", []string{"name", "email"}, []interface{}{"alice", "a@example.com"})
  _, err = client.Apply(ctx, []*spanner.Mutation{m})
  if err != nil {
    log.Fatal(err)
  }
}

func ExampleInsert(emu *EmulatorOptions) {
  m := spanner.Insert("Users", []string{"name", "email"}, []interface{}{"alice", "a@example.com"})
  _ = m // TODO: use with Client.Apply or in a ReadWriteTransaction.
}

func ExampleInsertMap(emu *EmulatorOptions) {
  m := spanner.InsertMap("Users", map[string]interface{}{
    "name":  "alice",
    "email": "a@example.com",
  })
  _ = m // TODO: use with Client.Apply or in a ReadWriteTransaction.
}

func ExampleInsertStruct(emu *EmulatorOptions) {
  type User struct {
    Name, Email string
  }
  u := User{Name: "alice", Email: "a@example.com"}
  m, err := spanner.InsertStruct("Users", u)
  if err != nil {
    log.Fatal(err)
  }
  _ = m // TODO: use with Client.Apply or in a ReadWriteTransaction.
}

func ExampleDelete(emu *EmulatorOptions) {
  m := spanner.Delete("Users", spanner.Key{"alice"})
  _ = m // TODO: use with Client.Apply or in a ReadWriteTransaction.
}

func ExampleDelete_keyRange(emu *EmulatorOptions) {
  m := spanner.Delete("Users", spanner.KeyRange{
    Start: spanner.Key{"alice"},
    End:   spanner.Key{"bob"},
    Kind:  spanner.ClosedClosed,
  })
  _ = m // TODO: use with Client.Apply or in a ReadWriteTransaction.
}

func ExampleRowIterator_Next(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  iter := client.Single().Query(ctx, spanner.NewStatement("SELECT FirstName FROM Singers"))
  defer iter.Stop()
  for {
    row, err := iter.Next()
    if err == iterator.Done {
      break
    }
    if err != nil {
      log.Fatal(err)
    }
    var firstName string
    if err := row.Column(0, &firstName); err != nil {
      log.Fatal(err)
    }
    fmt.Println(firstName)
  }
}

func ExampleRowIterator_Do(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  iter := client.Single().Query(ctx, spanner.NewStatement("SELECT FirstName FROM Singers"))
  err = iter.Do(func(r *spanner.Row) error {
    var firstName string
    if err := r.Column(0, &firstName); err != nil {
      return err
    }
    fmt.Println(firstName)
    return nil
  })
  if err != nil {
    log.Fatal(err)
  }
}

func ExampleRow_Size(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  row, err := client.Single().ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"name", "balance"})
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(row.Size()) // 2
}

func ExampleRow_ColumnName(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  row, err := client.Single().ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"name", "balance"})
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(row.ColumnName(1)) // "balance"
}

func ExampleRow_ColumnIndex(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  row, err := client.Single().ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"name", "balance"})
  if err != nil {
    log.Fatal(err)
  }
  index, err := row.ColumnIndex("balance")
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(index)
}

func ExampleRow_ColumnNames(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  row, err := client.Single().ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"name", "balance"})
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(row.ColumnNames())
}

func ExampleRow_ColumnByName(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  row, err := client.Single().ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"name", "balance"})
  if err != nil {
    log.Fatal(err)
  }
  var balance int64
  if err := row.ColumnByName("balance", &balance); err != nil {
    log.Fatal(err)
  }
  fmt.Println(balance)
}

func ExampleRow_Columns(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  row, err := client.Single().ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"name", "balance"})
  if err != nil {
    log.Fatal(err)
  }
  var name string
  var balance int64
  if err := row.Columns(&name, &balance); err != nil {
    log.Fatal(err)
  }
  fmt.Println(name, balance)
}

func ExampleRow_ToStruct(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  row, err := client.Single().ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"name", "balance"})
  if err != nil {
    log.Fatal(err)
  }

  type Account struct {
    Name    string
    Balance int64
  }

  var acct Account
  if err := row.ToStruct(&acct); err != nil {
    log.Fatal(err)
  }
  fmt.Println(acct)
}

func ExampleReadOnlyTransaction_Read(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  iter := client.Single().Read(ctx, "Users",
    spanner.KeySets(spanner.Key{"alice"}, spanner.Key{"bob"}),
    []string{"name", "email"})
  _ = iter // TODO: iterate using Next or Do.
}

func ExampleReadOnlyTransaction_ReadUsingIndex(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  iter := client.Single().ReadUsingIndex(ctx, "Users",
    "UsersByEmail",
    spanner.KeySets(spanner.Key{"a@example.com"}, spanner.Key{"b@example.com"}),
    []string{"name", "email"})
  _ = iter // TODO: iterate using Next or Do.
}

func ExampleReadOnlyTransaction_ReadWithOptions(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  // Use an index, and limit to 100 rows at most.
  iter := client.Single().ReadWithOptions(ctx, "Users",
    spanner.KeySets(spanner.Key{"a@example.com"}, spanner.Key{"b@example.com"}),
    []string{"name", "email"}, &spanner.ReadOptions{
      Index: "UsersByEmail",
      Limit: 100,
    })
  _ = iter // TODO: iterate using Next or Do.
}

func ExampleReadOnlyTransaction_ReadRow(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  row, err := client.Single().ReadRow(ctx, "Users", spanner.Key{"alice"},
    []string{"name", "email"})
  if err != nil {
    log.Fatal(err)
  }
  _ = row // TODO: use row
}

func ExampleReadOnlyTransaction_Query(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  iter := client.Single().Query(ctx, spanner.NewStatement("SELECT FirstName FROM Singers"))
  _ = iter // TODO: iterate using Next or Do.
}

func ExampleNewStatement(emu *EmulatorOptions) {
  stmt := spanner.NewStatement("SELECT FirstName, LastName FROM SINGERS WHERE LastName >= @start")
  stmt.Params["start"] = "Dylan"
  // TODO: Use stmt in Query.
}

func ExampleNewStatement_structLiteral(emu *EmulatorOptions) {
  stmt := spanner.Statement{
    SQL: `SELECT FirstName, LastName FROM SINGERS WHERE LastName = ("Lea", "Martin")`,
  }
  _ = stmt // TODO: Use stmt in Query.
}

func ExampleStructParam(emu *EmulatorOptions) {
  stmt := spanner.Statement{
    SQL: "SELECT * FROM SINGERS WHERE (FirstName, LastName) = @singerinfo",
    Params: map[string]interface{}{
      "singerinfo": struct {
        FirstName string
        LastName  string
      }{"Bob", "Dylan"},
    },
  }
  _ = stmt // TODO: Use stmt in Query.
}

func ExampleArrayOfStructParam(emu *EmulatorOptions) {
  stmt := spanner.Statement{
    SQL: "SELECT * FROM SINGERS WHERE (FirstName, LastName) IN UNNEST(@singerinfo)",
    Params: map[string]interface{}{
      "singerinfo": []struct {
        FirstName string
        LastName  string
      }{
        {"Ringo", "Starr"},
        {"John", "Lennon"},
      },
    },
  }
  _ = stmt // TODO: Use stmt in Query.
}

func ExampleReadOnlyTransaction_Timestamp(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  txn := client.Single()
  row, err := txn.ReadRow(ctx, "Users", spanner.Key{"alice"},
    []string{"name", "email"})
  if err != nil {
    log.Fatal(err)
  }
  readTimestamp, err := txn.Timestamp()
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println("read happened at", readTimestamp)
  _ = row // TODO: use row
}

func ExampleReadOnlyTransaction_WithTimestampBound(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  txn := client.Single().WithTimestampBound(spanner.MaxStaleness(30 * time.Second))
  row, err := txn.ReadRow(ctx, "Users", spanner.Key{"alice"}, []string{"name", "email"})
  if err != nil {
    log.Fatal(err)
  }
  _ = row // TODO: use row
  readTimestamp, err := txn.Timestamp()
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println("read happened at", readTimestamp)
}

func ExampleGenericColumnValue_Decode(emu *EmulatorOptions) {
  // In real applications, rows can be retrieved by methods like client.Single().ReadRow().
  row, err := spanner.NewRow([]string{"intCol", "strCol"}, []interface{}{42, "my-text"})
  if err != nil {
    log.Fatal(err)
  }
  for i := 0; i < row.Size(); i++ {
    var col spanner.GenericColumnValue
    if err := row.Column(i, &col); err != nil {
      log.Fatal(err)
    }
    switch col.Type.Code {
    case sppb.TypeCode_INT64:
      var v int64
      if err := col.Decode(&v); err != nil {
        log.Fatal(err)
      }
      fmt.Println("int", v)
    case sppb.TypeCode_STRING:
      var v string
      if err := col.Decode(&v); err != nil {
        log.Fatal(err)
      }
      fmt.Println("string", v)
    }
  }
  // Output:
  // int 42
  // string my-text
}

func ExampleClient_BatchReadOnlyTransaction(emu *EmulatorOptions) {
  ctx := context.Background()
  var (
    client *spanner.Client
    txn    *spanner.BatchReadOnlyTransaction
    err    error
  )
  if client, err = spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...); err != nil {
    log.Fatal(err)
  }
  defer client.Close()
  if txn, err = client.BatchReadOnlyTransaction(ctx, spanner.StrongRead()); err != nil {
    log.Fatal(err)
  }
  defer txn.Close()

  // Singer represents the elements in a row from the Singers table.
  type Singer struct {
    SingerID   int64
    FirstName  string
    LastName   string
    SingerInfo []byte
  }
  stmt := spanner.Statement{SQL: "SELECT * FROM Singers;"}
  partitions, err := txn.PartitionQuery(ctx, stmt, spanner.PartitionOptions{})
  if err != nil {
    log.Fatal(err)
  }
  // Note: here we use multiple goroutines, but you should use separate
  // processes/machines.
  wg := sync.WaitGroup{}
  for i, p := range partitions {
    wg.Add(1)
    go func(i int, p *spanner.Partition) {
      defer wg.Done()
      iter := txn.Execute(ctx, p)
      defer iter.Stop()
      for {
        row, err := iter.Next()
        if err == iterator.Done {
          break
        } else if err != nil {
          log.Fatal(err)
        }
        var s Singer
        if err := row.ToStruct(&s); err != nil {
          log.Fatal(err)
        }
        _ = s // TODO: Process the row.
      }
    }(i, p)
  }
  wg.Wait()
}

func ExampleCommitTimestamp(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }

  type account struct {
    User     string
    Creation spanner.NullTime // time.Time can also be used if column isNOT NULL
  }

  a := account{User: "Joe", Creation: spanner.NullTime{spanner.CommitTimestamp, true}}
  m, err := spanner.InsertStruct("Accounts", a)
  if err != nil {
    log.Fatal(err)
  }
  _, err = client.Apply(ctx, []*spanner.Mutation{m}, spanner.ApplyAtLeastOnce())
  if err != nil {
    log.Fatal(err)
  }

  if r, e := client.Single().ReadRow(ctx, "Accounts", spanner.Key{"Joe"}, []string{"User", "Creation"}); e != nil {
    log.Fatal(err)
  } else {
    var got account
    if err := r.ToStruct(&got); err != nil {
      log.Fatal(err)
    }
    _ = got // TODO: Process row.
  }
}

func ExampleStatement_regexpContains(emu *EmulatorOptions) {
  // Search for accounts with valid emails using regexp as per:
  //   https://cloud.google.com/spanner/docs/functions-and-operators#regexp_contains
  stmt := spanner.Statement{
    SQL: `SELECT * FROM users WHERE REGEXP_CONTAINS(email, @valid_email)`,
    Params: map[string]interface{}{
      "valid_email": `\Q@\E`,
    },
  }
  _ = stmt // TODO: Use stmt in a query.
}

func ExampleKeySets(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  // Get some rows from the Accounts table using a secondary index. In this case we get all users who are in Georgia.
  iter := client.Single().ReadUsingIndex(context.Background(), "Accounts", "idx_state", spanner.Key{"GA"}, []string{"state"})

  // Create a empty KeySet by calling the KeySets function with no parameters.
  ks := spanner.KeySets()

  // Loop the results of a previous query iterator.
  for {
    row, err := iter.Next()
    if err == iterator.Done {
      break
    } else if err != nil {
      log.Fatal(err)
    }
    var id string
    err = row.ColumnByName("User", &id)
    if err != nil {
      log.Fatal(err)
    }
    ks = spanner.KeySets(spanner.KeySets(spanner.Key{id}, ks))
  }

  _ = ks //TODO: Go use the KeySet in another query.

}

func ExampleNewReadWriteStmtBasedTransaction(emu *EmulatorOptions) {
  ctx := context.Background()
  client, err := spanner.NewClient(ctx, emu.uri, emu.ClientOptions()...)
  if err != nil {
    log.Fatal(err)
  }
  defer client.Close()

  f := func(tx *spanner.ReadWriteStmtBasedTransaction) error {
    var balance int64
    row, err := tx.ReadRow(ctx, "Accounts", spanner.Key{"alice"}, []string{"balance"})
    if err != nil {
      return err
    }
    if err := row.Column(0, &balance); err != nil {
      return err
    }

    if balance <= 10 {
      return errors.New("insufficient funds in account")
    }
    balance -= 10
    m := spanner.Update("Accounts", []string{"user", "balance"}, []interface{}{"alice", balance})
    return tx.BufferWrite([]*spanner.Mutation{m})
  }

  for {
    tx, err := spanner.NewReadWriteStmtBasedTransaction(ctx, client)
    if err != nil {
      log.Fatal(err)
      break
    }
    err = f(tx)
    if err != nil && status.Code(err) != codes.Aborted {
      tx.Rollback(ctx)
      log.Fatal(err)
      break
    } else if err == nil {
      _, err = tx.Commit(ctx)
      if err == nil {
        break
      } else if status.Code(err) != codes.Aborted {
        log.Fatal(err)
        break
      }
    }
    // Set a default sleep time if the server delay is absent.
    delay := 10 * time.Millisecond
    if serverDelay, hasServerDelay := spanner.ExtractRetryDelay(err); hasServerDelay {
      delay = serverDelay
    }
    time.Sleep(delay)
  }
}

func main() {
  exampleList := [...]string{
      "\n",
      "ExampleNewClient instance database\n",
      "ExampleClient_Single instance database\n",
      "ExampleClient_ReadOnlyTransaction instance database\n",
      "ExampleClient_ReadWriteTransaction instance database\n",
      "ExampleUpdate instance database\n",
      "ExampleUpdateMap instance database\n",
      "ExampleUpdateStruct instance database\n",
      "ExampleClient_Apply instance database\n",
      "ExampleInsert instance database\n",
      "ExampleInsertMap instance database\n",
      "ExampleInsertStruct instance database\n",
      "ExampleDelete instance database\n",
      "ExampleDelete_keyRange instance database\n",
      "ExampleRowIterator_Next instance database\n",
      "ExampleRowIterator_Do instance database\n",
      "ExampleRow_Size instance database\n",
      "ExampleRow_ColumnName instance database\n",
      "ExampleRow_ColumnIndex instance database\n",
      "ExampleRow_ColumnNames instance database\n",
      "ExampleRow_ColumnByName instance database\n",
      "ExampleRow_Columns instance database\n",
      "ExampleRow_ToStruct instance database\n",
      "ExampleReadOnlyTransaction_Read instance database\n",
      "ExampleReadOnlyTransaction_ReadUsingIndex instance database\n",
      "ExampleReadOnlyTransaction_ReadWithOptions instance database\n",
      "ExampleReadOnlyTransaction_ReadRow instance database\n",
      "ExampleReadOnlyTransaction_Query instance database\n",
      "ExampleNewStatement instance database\n",
      "ExampleNewStatement_structLiteral instance database\n",
      "ExampleStructParam instance database\n",
      "ExampleArrayOfStructParam instance database\n",
      "ExampleReadOnlyTransaction_Timestamp instance database\n",
      "ExampleReadOnlyTransaction_WithTimestampBound instance database\n",
      "ExampleGenericColumnValue_Decode instance database\n",
      "ExampleClient_BatchReadOnlyTransaction instance database\n",
      "ExampleCommitTimestamp instance database\n",
      "ExampleStatement_regexpContains instance database\n",
      "ExampleKeySets instance database\n",
      "ExampleNewReadWriteStmtBasedTransaction instance database\n"}

  args := os.Args
  if(len(args) != 4) {
    fmt.Println("Usage: <operation> <instance> <database>\n", exampleList)
    os.Exit(1)
  }

  example := args[1]
  instance := args[2]
  database := args[3]
  dbUri := fmt.Sprintf("projects/%v/instances/%v/databases/%v", "test-project", instance, database)
  emuOpts := &EmulatorOptions{
    hostport: "localhost:9010",
    uri: dbUri,
  }
  fmt.Println("**  ", args)

  switch example {
    case "ExampleNewClient":
      ExampleNewClient(emuOpts)
    case "ExampleClient_Single":
      ExampleClient_Single(emuOpts)
    case "ExampleClient_ReadOnlyTransaction":
      ExampleClient_ReadOnlyTransaction(emuOpts)
    case "ExampleClient_ReadWriteTransaction":
      ExampleClient_ReadWriteTransaction(emuOpts)
    case "ExampleUpdate":
      ExampleUpdate(emuOpts)
    case "ExampleUpdateMap":
      ExampleUpdateMap(emuOpts)
    case "ExampleUpdateStruct":
      ExampleUpdateStruct(emuOpts)
    case "ExampleClient_Apply":
      ExampleClient_Apply(emuOpts)
    case "ExampleInsert":
      ExampleInsert(emuOpts)
    case "ExampleInsertMap":
      ExampleInsertMap(emuOpts)
    case "ExampleInsertStruct":
      ExampleInsertStruct(emuOpts)
    case "ExampleDelete":
      ExampleDelete(emuOpts)
    case "ExampleDelete_keyRange":
      ExampleDelete_keyRange(emuOpts)
    case "ExampleRowIterator_Next":
      ExampleRowIterator_Next(emuOpts)
    case "ExampleRowIterator_Do":
      ExampleRowIterator_Do(emuOpts)
    case "ExampleRow_Size":
      ExampleRow_Size(emuOpts)
    case "ExampleRow_ColumnName":
      ExampleRow_ColumnName(emuOpts)
    case "ExampleRow_ColumnIndex":
      ExampleRow_ColumnIndex(emuOpts)
    case "ExampleRow_ColumnNames":
      ExampleRow_ColumnNames(emuOpts)
    case "ExampleRow_ColumnByName":
      ExampleRow_ColumnByName(emuOpts)
    case "ExampleRow_Columns":
      ExampleRow_Columns(emuOpts)
    case "ExampleRow_ToStruct":
      ExampleRow_ToStruct(emuOpts)
    case "ExampleReadOnlyTransaction_Read":
      ExampleReadOnlyTransaction_Read(emuOpts)
    case "ExampleReadOnlyTransaction_ReadUsingIndex":
      ExampleReadOnlyTransaction_ReadUsingIndex(emuOpts)
    case "ExampleReadOnlyTransaction_ReadWithOptions":
      ExampleReadOnlyTransaction_ReadWithOptions(emuOpts)
    case "ExampleReadOnlyTransaction_ReadRow":
      ExampleReadOnlyTransaction_ReadRow(emuOpts)
    case "ExampleReadOnlyTransaction_Query":
      ExampleReadOnlyTransaction_Query(emuOpts)
    case "ExampleNewStatement":
      ExampleNewStatement(emuOpts)
    case "ExampleNewStatement_structLiteral":
      ExampleNewStatement_structLiteral(emuOpts)
    case "ExampleStructParam":
      ExampleStructParam(emuOpts)
    case "ExampleArrayOfStructParam":
      ExampleArrayOfStructParam(emuOpts)
    case "ExampleReadOnlyTransaction_Timestamp":
      ExampleReadOnlyTransaction_Timestamp(emuOpts)
    case "ExampleReadOnlyTransaction_WithTimestampBound":
      ExampleReadOnlyTransaction_WithTimestampBound(emuOpts)
    case "ExampleGenericColumnValue_Decode":
      ExampleGenericColumnValue_Decode(emuOpts)
    case "ExampleClient_BatchReadOnlyTransaction":
      ExampleClient_BatchReadOnlyTransaction(emuOpts)
    case "ExampleCommitTimestamp":
      ExampleCommitTimestamp(emuOpts)
    case "ExampleStatement_regexpContains":
      ExampleStatement_regexpContains(emuOpts)
    case "ExampleKeySets":
      ExampleKeySets(emuOpts)
    case "ExampleNewReadWriteStmtBasedTransaction":
      ExampleNewReadWriteStmtBasedTransaction(emuOpts)
    default:
      fmt.Println("Unrecoginized option. See list of valid examples:\n", exampleList)
  }
}
