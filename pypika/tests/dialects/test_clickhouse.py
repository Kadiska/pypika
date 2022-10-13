from unittest import TestCase

from pypika import (
    ClickHouseQuery,
    Database,
    Table,
    Order,
    functions as fn,
)


class ClickHouseQueryTests(TestCase):
    t = Table('abc')

    def test_use_AS_keyword_for_alias(self):
        query = ClickHouseQuery.from_(self.t).select(self.t.foo.as_('f1'), self.t.bar.as_('f2'))
        self.assertEqual(str(query), 'SELECT "foo" AS "f1","bar" AS "f2" FROM "abc"')

    def test_use_SAMPLE_keyword(self):
        query = ClickHouseQuery.from_(self.t).select(self.t.foo).sample(10)
        self.assertEqual(str(query), 'SELECT "foo" FROM "abc" SAMPLE 10')

    def test_use_SAMPLE_with_offset_keyword(self):
        query = ClickHouseQuery.from_(self.t).select(self.t.foo).sample(10, 5)
        self.assertEqual(str(query), 'SELECT "foo" FROM "abc" SAMPLE 10 OFFSET 5')

    def test_use_FINAL_keyword(self):
        query = ClickHouseQuery.from_(self.t).select(self.t.foo).final()
        self.assertEqual(str(query), 'SELECT "foo" FROM "abc" FINAL')

    def test_orderby_single_field(self):
        q = ClickHouseQuery.from_(self.t).orderby(self.t.foo).select(self.t.foo)

        self.assertEqual('SELECT "foo" FROM "abc" ORDER BY "foo"', str(q))

    def test_orderby_multi_fields(self):
        q = ClickHouseQuery.from_(self.t).orderby(self.t.foo).orderby(self.t.bar).select(self.t.foo, self.t.bar)

        self.assertEqual('SELECT "foo","bar" FROM "abc" ORDER BY "foo","bar"', str(q))

    def test_orderby_asc(self):
        q = ClickHouseQuery.from_(self.t).orderby(self.t.foo, order=Order.asc).select(self.t.foo)

        self.assertEqual('SELECT "foo" FROM "abc" ORDER BY "foo" ASC', str(q))

    def test_orderby_desc(self):
        q = ClickHouseQuery.from_(self.t).orderby(self.t.foo, order=Order.desc).select(self.t.foo)

        self.assertEqual('SELECT "foo" FROM "abc" ORDER BY "foo" DESC', str(q))

    def test_orderby_no_alias(self):
        bar = self.t.bar.as_("bar01")
        q = ClickHouseQuery.from_(self.t).select(fn.Sum(self.t.foo), bar).orderby(bar)

        self.assertEqual(
            'SELECT SUM("foo"),"bar" AS "bar01" FROM "abc" ORDER BY "bar"',
            q.get_sql(orderby_alias=False),
        )

    def test_orderby_with_fill(self):
        bar = self.t.bar.as_("bar01")
        q = ClickHouseQuery.from_(self.t).select(fn.Sum(self.t.foo), bar).orderby(bar, with_fill=True)

        self.assertEqual(
            'SELECT SUM("foo"),"bar" AS "bar01" FROM "abc" ORDER BY "bar" WITH FILL',
            q.get_sql(),
        )

    def test_orderby_with_fill_and_step(self):
        bar = self.t.bar.as_("bar01")
        q = ClickHouseQuery.from_(self.t).select(fn.Sum(self.t.foo), bar).orderby(bar, with_fill=True, step=5)

        self.assertEqual(
            'SELECT SUM("foo"),"bar" AS "bar01" FROM "abc" ORDER BY "bar" WITH FILL STEP 5',
            q.get_sql(),
        )


    def test_orderby_with_fill_from_step(self):
        bar = self.t.bar.as_("bar01")
        q = ClickHouseQuery.from_(self.t).select(fn.Sum(self.t.foo), bar).orderby(bar, with_fill=True, step=5, from_=0, to=10)

        self.assertEqual(
            'SELECT SUM("foo"),"bar" AS "bar01" FROM "abc" ORDER BY "bar" WITH FILL FROM 0 TO 10 STEP 5',
            q.get_sql(),
        )


class ClickHouseDeleteTests(TestCase):
    table_abc = Table("abc")

    def test_omit_where(self):
        q = ClickHouseQuery.from_("abc").delete()

        self.assertEqual('ALTER TABLE "abc" DELETE', str(q))

    def test_omit_where__table_schema(self):
        q = ClickHouseQuery.from_(Table("abc", "schema1")).delete()

        self.assertEqual('ALTER TABLE "schema1"."abc" DELETE', str(q))

    def test_where_field_equals(self):
        q1 = ClickHouseQuery.from_(self.table_abc).where(self.table_abc.foo == self.table_abc.bar).delete()
        q2 = ClickHouseQuery.from_(self.table_abc).where(self.table_abc.foo.eq(self.table_abc.bar)).delete()

        self.assertEqual('ALTER TABLE "abc" DELETE WHERE "foo"="bar"', str(q1))
        self.assertEqual('ALTER TABLE "abc" DELETE WHERE "foo"="bar"', str(q2))


class ClickHouseUpdateTests(TestCase):
    table_abc = Table("abc")

    def test_update(self):
        q = ClickHouseQuery.update(self.table_abc).where(self.table_abc.foo == 0).set("foo", "bar")

        self.assertEqual('ALTER TABLE "abc" UPDATE "foo"=\'bar\' WHERE "foo"=0', str(q))


class ClickHouseDropQuery(TestCase):
    table_abc = Table("abc")
    database_xyz = Database("mydb")
    cluster_name = "mycluster"

    def test_drop_database(self):
        q1 = ClickHouseQuery.drop_database(self.database_xyz)
        q2 = ClickHouseQuery.drop_database(self.database_xyz).on_cluster(self.cluster_name)
        q3 = ClickHouseQuery.drop_database(self.database_xyz).if_exists().on_cluster(self.cluster_name)

        self.assertEqual('DROP DATABASE "mydb"', str(q1))
        self.assertEqual('DROP DATABASE "mydb" ON CLUSTER "mycluster"', str(q2))
        self.assertEqual('DROP DATABASE IF EXISTS "mydb" ON CLUSTER "mycluster"', str(q3))

    def test_drop_table(self):
        q1 = ClickHouseQuery.drop_table(self.table_abc)
        q2 = ClickHouseQuery.drop_table(self.table_abc).on_cluster(self.cluster_name)
        q3 = ClickHouseQuery.drop_table(self.table_abc).if_exists().on_cluster(self.cluster_name)

        self.assertEqual('DROP TABLE "abc"', str(q1))
        self.assertEqual('DROP TABLE "abc" ON CLUSTER "mycluster"', str(q2))
        self.assertEqual('DROP TABLE IF EXISTS "abc" ON CLUSTER "mycluster"', str(q3))

    def test_drop_dictionary(self):
        q1 = ClickHouseQuery.drop_dictionary("dict")
        q2 = ClickHouseQuery.drop_dictionary("dict").on_cluster(self.cluster_name)
        q3 = ClickHouseQuery.drop_dictionary("dict").if_exists().on_cluster(self.cluster_name)

        self.assertEqual('DROP DICTIONARY "dict"', str(q1))
        self.assertEqual('DROP DICTIONARY "dict"', str(q2))  # NO CLUSTER
        self.assertEqual('DROP DICTIONARY IF EXISTS "dict"', str(q3))  # NO CLUSTER

    def test_drop_other(self):
        q1 = ClickHouseQuery.drop_quota("myquota")
        q2 = ClickHouseQuery.drop_user("myuser")
        q3 = ClickHouseQuery.drop_view("myview")

        self.assertEqual('DROP QUOTA "myquota"', str(q1))
        self.assertEqual('DROP USER "myuser"', str(q2))
        self.assertEqual('DROP VIEW "myview"', str(q3))
