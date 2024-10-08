ts=`date +%Y%m%d%H%M%S`
tagname="kongming_production_$ts"
echo "creating tag:$tagname"
git tag $tagname
git push origin $tagname