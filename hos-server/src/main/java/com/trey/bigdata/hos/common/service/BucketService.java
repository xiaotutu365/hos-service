package com.trey.bigdata.hos.common.service;


import com.trey.bigdata.hos.core.usermgr.model.UserInfo;
import com.trey.bigdata.hos.common.BucketModel;
import com.trey.bigdata.hos.common.dao.BucketModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Transactional
@Service
public class BucketService {

    @Autowired
    private BucketModelMapper bucketModelMapper;

    public boolean addBucket(UserInfo userInfo, String bucketName, String detail) {
        BucketModel bucketModel = new BucketModel(bucketName, userInfo.getUserName(), detail);
        bucketModelMapper.addBucket(bucketModel);
        return true;
    }

    /**
     *
     * @param bucketName
     * @return
     */
    public boolean deleteBucket(String bucketName) {
        bucketModelMapper.deleteBucket(bucketName);
        // todo delete auth for bucket
        return true;
    }

    public boolean updateBucket(String bucketName, String detail) {
        bucketModelMapper.updateBucket(bucketName, detail);
        return true;
    }

    public BucketModel getBucketById(String bucketId) {
        return bucketModelMapper.getBucket(bucketId);
    }

    public BucketModel getBucketByName(String bucketName) {
        return bucketModelMapper.getBucketByName(bucketName);
    }

    public List<BucketModel> getBucketByCreator(String creator) {
        return bucketModelMapper.getBucketByCreator(creator);
    }

    public List<BucketModel> getUserBuckets(String token) {
        // todo
        return null;
    }
}