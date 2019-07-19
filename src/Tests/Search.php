<?php

namespace Modules\Elasticsearch\Tests;

use \Modules\Elasticsearch\Services\Elasticsearch;
use Elasticsearch\ClientBuilder;

class search
{

	/**
	 * 创建索引
	 */
	public function createIndexAction()
	{
		try {

			$type  = 'account_detail_type4';
			$index = 'account_index4';

			$properties = [
				'id'                   => [
					'type' => 'integer',
				],
				'type'                 => [
					'type' => 'integer',
				],
				'accountStatementId'   => [
					'type' => 'integer',
				],
				'sourceno'             => [
					'type'     => 'text',
					'analyzer' => 'standard',
				],
				'accountStatementCode' => [
					'type'     => 'text',
					'analyzer' => 'standard',
				],
				'applyPaymentCode'     => [
					'type'     => 'text',
					'analyzer' => 'standard',
				],
				'skuIds'               => [
					'type'     => 'text',
					'analyzer' => 'standard',
				],
				'sellerIds'            => [
					'type'     => 'text',
					'analyzer' => 'standard',
				],
				'createTime'           => [
					'type' => 'integer',
				],
				'supplierId'           => [
					'type' => 'integer',
				],
				'createType'           => [
					'type' => 'integer',
				],
				'status'               => [
					'type' => 'integer',
				],
				'accountType'          => [
					'type' => 'integer',
				],
				'grossProfitError'     => [
					'type' => 'text',
				],
			];
			//$reponse = $client->index($params);
			$search   = new \Modules\Elasticsearch\Services\Elasticsearch();
			$response = $search->createIndex($index, $type, $properties);

			print_r($response);

		} catch (\Exception $e) {
			print_r($e->getTraceAsString());
			print_r($e->getMessage());
		} catch (\Error $e) {
			print_r($e->getTraceAsString());
			print_r($e->getMessage());
		}

	}

	/**
	 * 索引文档
	 */
	public function bulkDocsAction()
	{
		$minId = 0;
		$maxId = 1000;
		try {

			while ($maxId) {
				$data = \AccountStatement::find([
					'accountStatementId >= :minId: and accountStatementId <= :maxId:',
					'bind' => [
						'minId' => $minId,
						'maxId' => $maxId,
					],
				]);

				$maxId += 1000;
				$minId += 1000;

				if ($maxId >= 23000000) {
					$maxId = 0;
				}

				if ($data) {
					$params = $data->toArray();
				}

				foreach ($params as $k => $v) {

					$accountDetails = \AccountStatementDetail::find([
						'accountStatementId = ' . $v['accountStatementId'],
					]);
					$accountDetails = $accountDetails->toArray();

					$params[$k]['skuIds']    = implode(' ', array_unique(array_column($accountDetails, 'skuId')));
					$params[$k]['sourceno']  = implode(' ', array_unique(array_column($accountDetails, 'sourceno')));
					$params[$k]['sellerIds'] = implode(' ', array_unique(array_column($accountDetails, 'sellerId')));

					$pplyPayment = \ApplyPayment::findFirst([
						'accountStatementId = ' . $v['accountStatementId'],
					]);

					$params[$k]['applyPaymentCode'] = $pplyPayment->applyPaymentCode;

				}

				$index['index'] = 'account_index4';
				$index['type']  = 'account_detail_type4';

				/**
				 * @var \Modules\Elasticsearch\Services\Elasticsearch;
				 */
				$client = new \Modules\Elasticsearch\Services\Elasticsearch();

				$reponse = $client->bulkAll($params, $index);

				print_r(count($reponse));

			}
		} catch (\Exception $e) {
			print_r($e->getMessage());
			print_r($e->getTraceAsString());
		} catch (\Error $e) {
			print_r($e->getMessage());
			print_r($e->getTraceAsString());
		}


	}

	public function getTextAction()
	{
		$hosts = [
			'172.17.0.6:9200',         // IP + Port
		];
		try {
			$client = ClientBuilder::create()->setHosts($hosts)->build();
			$params = [
				'index' => 'account_index4',
				'type'  => 'account_detail_type4',
				'id'    => 3233,
			];

			$response = $client->get($params);
			print_r($response);

		} catch (\Exception $e) {
			print_r($e->getMessage());
			die;
		} catch (\Error $e) {
			print_r($e->getMessage());
			die;
		}


	}


	/**
	 * 搜索
	 */
	public function searchAction()
	{
		try {
			$size   = 10;
			$offset = 0;

			$client = new \Modules\Elasticsearch\Services\Elasticsearch();

			$params = [
				'index' => 'account_index4',
				'type'  => 'account_detail_type4',
			];

			$client->andWhere('skuIds', '4113')
				->setIndex($params)
				->setOrderBy('id')
				//->rangeWhere('id', 'gte', 90, 'lt', 99)
				->setLimit(0, 10);
			print_r($client->getParams());
			$response = $client->search();
			print_r($response);
			die;
		} catch (\Exception $e) {
			print_r($e->getMessage());
			print_r($e->getTraceAsString());
		} catch (\Error $e) {
			print_r($e->getMessage());
			print_r($e->getTraceAsString());
		}

	}

	public function getMapAction()
	{
		$parms['index'] = 'account_index4';

		$result = new  \Modules\Elasticsearch\Services\Elasticsearch();
		$re     = $result->getIndexMapping($parms);

		print_r($re);
	}

	public function deleteTextAction()
	{
		$hosts  = [
			'172.17.0.6:9200',         // IP + Port
		];
		$client = ClientBuilder::create()->setHosts($hosts)->build();

		$params = [
			'index' => 'account_index4',
			'type'  => 'my_type',
			'id'    => 'my_id',
		];

		$response = $client->delete($params);
		print_r($response);
	}

	public function deleteIndexAction()
	{

		$hosts  = [
			'172.17.0.6:9200',         // IP + Port
		];
		$client = ClientBuilder::create()->setHosts($hosts)->build();

		$deleteParams = [
			'index' => 'account_index4',
		];
		$response     = $client->indices()->delete($deleteParams);
		print_r($response);
	}

	public function indexTextAction()
	{
		$hosts  = [
			'172.17.0.6:9200',         // IP + Port
		];
		$client = ClientBuilder::create()->setHosts($hosts)->build();

		$params = [
			'index' => 'my_index',
			'body'  => [
				'settings' => [
					'number_of_shards'   => 2,
					'number_of_replicas' => 0,
				],
			],
		];

		$response = $client->indices()->create($params);
		print_r($response);
	}

}
