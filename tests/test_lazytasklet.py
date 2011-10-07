import unittest

import dev_appserver
dev_appserver.fix_sys_path()

from google.appengine.ext import testbed
from ndb import model, tasklets

from lazytasklet import KeyFutureValueHolder, lazytasklet, AutoKeyProperty


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()

    def create_users(self):
        self.u1 = User(name='u1')
        self.u1.put()
        self.u2 = User(name='u2')
        self.u2.put()

    def create_articles(self):
        self.a1 = Article(title='a1', created_by_key=self.u1.key,
            editors_keys=[self.u1.key])
        self.a1.put()
        self.a2 = Article(title='a2', created_by_key=self.u2.key,
            editors_keys=[self.u2.key, self.u1.key])
        self.a2.put()

    def create_new_articles(self):
        self.a1 = NewArticle(title='a1', created_by=self.u1,
            editors=[self.u1])
        self.a1.put()
        self.a2 = NewArticle(title='a2', created_by=self.u2,
            editors=[self.u2, self.u1])
        self.a2.put()

    def assert_articles(self, articles):
        self.assertEqual(len(articles), 2)

        self.assertEqual(articles[0].key, self.a1.key)
        self.assertEqual(articles[1].key, self.a2.key)

        self.assertEqual(articles[0].created_by.key, self.u1.key)
        self.assertEqual(articles[1].created_by.key, self.u2.key)

        self.assertEqual(articles[0].created_by.value, self.u1)
        self.assertEqual(articles[1].created_by.value, self.u2)

        self.assertEqual(len(articles[0].editors), 1)
        self.assertEqual(articles[0].editors[0].key, self.u1.key)
        self.assertEqual(articles[0].editors[0].value, self.u1)
        self.assertEqual(len(articles[1].editors), 2)
        self.assertEqual(articles[1].editors[0].key, self.u2.key)
        self.assertEqual(articles[1].editors[0].value, self.u2)
        self.assertEqual(articles[1].editors[1].key, self.u1.key)
        self.assertEqual(articles[1].editors[1].value, self.u1)

        self.assertEqual(articles[0].articles_from_author_count.value, 1)
        self.assertEqual(articles[1].articles_from_author_count.value, 1)


class User(model.Model):
    name = model.StringProperty(required=True)


class Article(model.Model):
    title = model.StringProperty(required=True)
    created_by_key = model.KeyProperty(required=True)
    editors_keys = model.KeyProperty(repeated=True)

    @classmethod
    @lazytasklet
    def select_related_async(cls, articles):
        for article in articles:
            article.created_by = yield article.created_by_key.get_async()
            article.editors = yield model.get_multi_async(article.editors_keys)
            article.articles_from_author_count = yield cls.query() \
                .filter(cls.created_by_key == article.created_by_key) \
                .count_async()
        raise tasklets.Return(articles)


class NewArticle(model.Model):
    title = model.StringProperty(required=True)
    created_by = AutoKeyProperty(required=True)
    editors = AutoKeyProperty(repeated=True)

    @classmethod
    @lazytasklet
    def select_related_async(cls, articles):
        for article in articles:
            yield article.created_by
            yield article.editors
            article.articles_from_author_count = yield cls.query() \
                .filter(cls.created_by == article.created_by) \
                .count_async()
        raise tasklets.Return(articles)


class TestKeyFutureProperty(BaseTestCase):
    def test_init_with_key(self):
        u1 = User(name='u1')
        u1.put()

        holder = KeyFutureValueHolder(u1.key)
        self.assertEqual(holder.key, u1.key)
        self.assertEqual(holder.value, u1)

    def test_init_with_future(self):
        u1 = User(name='u1')
        u1.put()
        u1_future = u1.key.get_async()

        holder = KeyFutureValueHolder(u1_future)
        self.assertEqual(holder.value, u1)
        self.assertEqual(holder.key, u1.key)

    def test_init_with_value(self):
        u1 = User(name='u1')
        u1.put()

        holder = KeyFutureValueHolder(u1)
        self.assertEqual(holder.value, u1)
        self.assertEqual(holder.key, u1.key)


class TestAutoKeyProperty(BaseTestCase):
    def test_auto_key_property_without_lazytasklet(self):
        self.create_users()
        self.create_new_articles()

        articles = NewArticle.query().fetch()
        for article in articles:
            article.articles_from_author_count = yield NewArticle.query() \
                .filter(NewArticle.created_by_key == article.created_by_key) \
                .count_async()

        self.assert_articles(articles)


class TestLazyTasklet(BaseTestCase):
    def test_lazytasklet_with_key_property(self):
        self.create_users()
        self.create_articles()

        articles = Article.query().fetch()
        articles = Article.select_related_async(articles).get_result()
        self.assert_articles(articles)

    def test_lazytasklet_with_auto_key_property(self):
        self.create_users()
        self.create_new_articles()

        articles = NewArticle.query().fetch()
        articles = NewArticle.select_related_async(articles).get_result()
        self.assert_articles(articles)
