<?php
/**
 * Elasticsearch 数据搜索引擎
 */

namespace Modules\Elasticsearch\Services;

use Elasticsearch\ClientBuilder;

class Elasticsearch
{
	/**
	 * 配置
	 * @var array
	 */
	private $config = [
		'hosts' => ['172.17.0.6:9200'],
	];

	/**
	 * 查询参数
	 * @var array
	 */
	private $params = [];
	/**
	 * Elasticsearch 搜索api
	 * @var \Elasticsearch\Client
	 */
	private $api;

	public function __construct($config = [])
	{
		if(!empty($config)){
			$this->config = $config;
		}
		$this->api = ClientBuilder::create()->setHosts($this->config['hosts'])->build();
	}

	/**
	 * 索引一个文档
	 * 说明：索引没有被创建时会自动创建索引
	 */
	public function addOne(array $params)
	{
		return $this->api->index($params);
	}

	/**
	 * 索引多个文档
	 * 说明：索引没有被创建时会自动创建索引
	 * @param array $documents
	 * @param array $index
	 * @return array
	 */
	/**
	 * @param array $documents
	 * @param array $index
	 * @return array
	 */
	public function bulkAll(array $documents, array $index)
	{
		$params = [];
		foreach ($documents as $k => $v) {
			$params['body'][] = [
				'index' => [
					'_index' => $index['index'],
					'_type'  => $index['type'],
					'_id'    => $v['id'],
				],
			];

			$params['body'][] = $v;

			if ($k % 1000 == 0) {
				$responses = $this->api->bulk($params);
				$params    = ['body' => []];
				unset($responses);
			}
		}
		$responses = [];
		if (!empty($params['body'])) {
			$responses = $this->api->bulk($params);
		}

		return $responses;
	}

	/**
	 * 获取一个文档
	 * @param $index
	 * @param $type
	 * @param $id
	 * @return array
	 */
	public function getOne($index, $type, $id)
	{
		$params          = [];
		$params['index'] = $index;
		$params['type']  = $type;
		$params['id']    = $id;

		return $this->api->get($params);
	}

	/**
	 * 设置排序
	 * @param $field
	 * @param string $orderBy
	 * @return $this
	 */
	public function setOrderBy($field, $orderBy = 'desc')
	{
		$this->params['body']['sort'] = [$field => ['order' => $orderBy]];

		return $this;
	}

	/**
	 * 设置分页
	 * @param $offset
	 * @param $size
	 * @return $this
	 */
	public function setLimit($offset = 0, $size = 10)
	{
		$this->params['from'] = $offset;
		$this->params['size'] = $size;

		return $this;
	}

	/**
	 * sql中的and 操作
	 * @param $field
	 * @param $value
	 * @return $this
	 */
	public function andWhere($field, $value)
	{
		$this->params['body']['query']["bool"]['must'][] = ["match" => [$field => $value]];

		return $this;
	}

	/**
	 * sql 中的or操作
	 * @param $field
	 * @param $value
	 * @return $this
	 */
	public function orWhere($field, $value)
	{
		$this->params['body']['query']["bool"]['should'][] = ["match" => [$field => $value]];

		return $this;
	}

	/**
	 * not 操作
	 * @param $field
	 * @param $value
	 * @return $this
	 */
	public function notWhere($field, $value)
	{
		$this->params['body']['query']["bool"]['must_not'][] = ["match" => [$field => $value]];

		return $this;
	}

	/**
	 * 范围操作
	 * gt: > 大于
	 * lt: < 小于
	 * gte: >= 大于或等于
	 * lte: <= 小于或等于
	 * @param string $filed
	 * @param string $frontSymbol
	 * @param string $frontValue
	 * @param string $afterSymbol
	 * @param string $afterValue
	 * @return $this
	 */
	public function rangeWhere($filed, $frontSymbol = '', $frontValue = '', $afterSymbol = '', $afterValue = '')
	{
		if (isset($this->params['body']['query']['bool']['filter']['range'])) {
			$this->params['body']['query']['bool']['filter']['range'] = array_merge($this->params['body']['query']['bool']['filter']['range'],
				[
					$filed => [
						$frontSymbol => $frontValue,
						$afterSymbol => $afterValue,
					],
				]);
		} else {
			$this->params['body']['query']['bool']['filter']['range'] = [
				$filed => [
					$frontSymbol => $frontValue,
					$afterSymbol => $afterValue,
				],
			];
		}

		return $this;
	}

	/**
	 * 获取参数
	 * @return array
	 */
	public function getParams()
	{
		return $this->params;
	}

	/**
	 * 设置索引
	 * @param $params
	 * @return $this
	 */
	public function setIndex($params)
	{
		$this->params['index'] = $params['index'];
		$this->params['type']  = $params['type'];

		return $this;
	}

	/**
	 * 搜索文档
	 */
	public function search()
	{
		if (empty($this->params)) {
			return [];
		}

		return $this->api->search($this->params);
	}

	/**
	 * 获取映射
	 * @param array $params
	 * @return array
	 */
	public function getMap($params = [])
	{
		return $this->api->indices()->getMapping($params);
	}

	/**
	 * 删除文档
	 * 说明：文档删除后，不会删除对应索引。
	 */
	public function delete()
	{
		$params          = [];
		$params['index'] = 'account_index';
		$params['type']  = 'cat';
		$params['id']    = '20180407001';

		return $this->api->delete($params);
	}


	/**
	 * 创建索引
	 * [
	 * 'accountStatementDetailId' => [
	 * 'type' => 'integer',
	 * ],
	 * 'type'                     => [
	 * 'type' => 'integer',
	 * ],
	 * 'sourceno'                 => [
	 * 'type' => 'string',
	 * ],
	 * 'createTime'               => [
	 * 'type' => 'integer',
	 * ],
	 * ],
	 */
	/**
	 * @param $indexName
	 * @param $type
	 * @param $properties
	 * @return array
	 */
	public function createIndex($indexName, $type, $properties)
	{
		$params = [
			'index' => $indexName,
			'body'  => [
				//分布式设置
//				'settings' => [
//					'number_of_shards'   => 3,
//					'number_of_replicas' => 2,
//				],
				'mappings' => [
					$type => [
						'_source'    => [
							'enabled' => true,
						],
						'properties' => $properties,
					],
				],
			],
		];

		$response = $this->api->indices()->create($params);

		return $response;
	}

	/**
	 * 删除索引：匹配单个 | 匹配多个
	 * 说明： 索引删除后，索引下的所有文档也会被删除
	 */
	public function deleteIndex()
	{
		$params          = [];
		$params['index'] = 'test_index';  # 删除test_index单个索引
		#$params['index'] = 'test_index*'; # 删除以test_index开始的所有索引
		return $this->api->indices()->delete($params);
	}


	/**
	 * 设置索引配置
	 */
	public function setIndexConfig()
	{
		$params          = [];
		$params['index'] = 'account_test';

		$params['body']['index']['number_of_replicas'] = 0;
		$params['body']['index']['refresh_interval']   = -1;

		return $this->api->indices()->putSettings($params);
	}

	/**
	 * 获取索引配置
	 * 单个获取条件写法
	 * 多个获取条件写法
	 * $params['index'] = ['account', 'test_index'];
	 * @param array $index
	 * @return array
	 */
	public function getIndexConfig(array $index)
	{

		$params['index'] = $index;

		return $this->api->indices()->getSettings($params);
	}

	/**
	 * 设置索引映射
	 */
	public function setIndexMapping()
	{
		#  设置索引和类型
		$params['index'] = 'xiaochuan';
		$params['type']  = 'cat';

		#  向现有索引添加新类型
		$myTypeMapping         = [
			'_source'    => [
				'enabled' => true,
			],
			'properties' => [
				'first_name' => [
					'type'     => 'string',
					'analyzer' => 'standard',
				],
				'age'        => [
					'type' => 'integer',
				],
			],
		];
		$params['body']['cat'] = $myTypeMapping;

		#  更新索引映射
		$this->api->indices()->putMapping($params);
	}

	/**
	 * 获取索引映射
	 * 获取索引为：account的映射
	 *  $params['index'] = 'account';
	 * 获取类型为：detail的映射
	 *  $params['type'] = 'detail';
	 * 获取（索引为：account和 类型为：detail）的映射
	 *    $params['index'] = 'account';
	 *    $params['type']  = 'detail'
	 * 获取索引为：xiaochuan和test_index的映射
	 *  $params['index'] = ['account', 'test_index'];
	 * @param $params
	 * @return array
	 */
	public function getIndexMapping($params)
	{
		$ret = $this->api->indices()->getMapping($params);

		return $ret;
	}

}